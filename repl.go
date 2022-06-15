package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"examplestorage/proto"

	"github.com/google/shlex"
	"golang.org/x/term"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var help = `
This interface allows you to run RPCs and quorum calls against the Storage
Servers interactively. Take a look at the files 'client.go' and 'server.go'
for the source code of the RPC handlers and quorum functions.
The following commands can be used:

help                            Show this text
exit                            Exit the program
nodes                           Print a list of the available nodes
rpc    [node index] [operation]	Executes an RPC on the given node.
qc     [operation]             	Executes a quorum call on all nodes.
mcast  [key] [value]            Executes a multicast write call on all nodes.
cfg    [config]              	Updates the default configuration.
reconf [config]              	Reconfigure to new configuration.

The following operations are supported:

read 	[key]        	Read a value
write	[key] [value]	Write a value

Examples:

> rpc 0 write foo bar
The command performs the 'write' RPC on node 0, and sets 'foo' = 'bar'

> qc read foo
The command performs the 'read' quorum call, and returns the value of 'foo'

> cfg 1:3 
Updates to configuration with nodes 1 and 2

> cfg 0,2
Updates to configuration with nodes 0 and 2
`

type repl struct {
	*client
	term *term.Terminal
}

func newRepl(c *client) *repl {
	return &repl{
		client: c,
		term: term.NewTerminal(struct {
			io.Reader
			io.Writer
		}{os.Stdin, os.Stderr}, "> "),
	}
}

// ReadLine reads a line from the terminal in raw mode.
//
// FIXME: ReadLine currently does not work with arrow keys on windows for some reason
// See: https://stackoverflow.com/questions/58237670/terminal-raw-mode-does-not-support-arrows-on-windows
func (r repl) ReadLine() (string, error) {
	fd := int(os.Stdin.Fd())
	oldState, err := term.MakeRaw(fd)
	if err != nil {
		panic(err)
	}
	defer func() {
		err := term.Restore(fd, oldState)
		if err != nil {
			panic(err)
		}
	}()

	return r.term.ReadLine()
}

// Repl runs an interactive Read-eval-print loop, that allows users to run commands that perform
// RPCs and quorum calls using the manager and configuration.
func Repl(c *client) {
	r := newRepl(c)

	fmt.Println(help)
	for {
		l, err := r.ReadLine()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read line: %v\n", err)
			os.Exit(1)
		}
		args, err := shlex.Split(l)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to split command: %v\n", err)
			os.Exit(1)
		}
		if len(args) < 1 {
			continue
		}

		switch args[0] {
		case "exit":
			fallthrough
		case "quit":
			return
		case "help":
			fmt.Println(help)
		case "rpc":
			r.rpc(args[1:])
		case "qc":
			r.qc(args[1:])
		case "cfg":
			r.cfgc(args[1:])
		case "reconf":
			r.reconf(args[1:])
		case "mcast":
			fallthrough
		case "multicast":
			r.multicast(args[1:])
		case "nodes":
			fmt.Println("Nodes: ")
			for i, n := range r.mgr.Nodes() {
				fmt.Printf("%d: %s\n", i, n.Address())
			}
		default:
			fmt.Printf("Unknown command '%s'. Type 'help' to see available commands.\n", args[0])
		}
	}
}

func (r repl) rpc(args []string) {
	if len(args) < 2 {
		fmt.Println("'rpc' requires a node index and an operation.")
		return
	}

	index, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Printf("Invalid id '%s'. node index must be numeric.\n", args[0])
		return
	}

	if index < 0 || index >= len(r.mgr.Nodes()) {
		fmt.Printf("Invalid index. Must be between 0 and %d.\n", len(r.mgr.Nodes())-1)
		return
	}

	node := r.mgr.Nodes()[index]

	switch args[1] {
	case "read":
		r.readRPC(args[2:], node)
	case "write":
		r.writeRPC(args[2:], node)
	case "list":
		r.listKeysRPC(node)
	}
}

func (r repl) reconf(args []string) {
	if len(args) < 1 {
		fmt.Println("'reconf' requires a configuration.")
		return
	}
	r.client.reconf(args[0])
	fmt.Println("Reconfiguration finished")
}

func (r repl) readRPC(args []string, node *proto.Node) {
	if len(args) < 1 {
		fmt.Println("Read requires a key to read.")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := node.ReadRPC(ctx, &proto.ReadRequest{Key: args[0]})
	cancel()
	if err != nil {
		fmt.Printf("Read RPC finished with error: %v\n", err)
		return
	}
	if !resp.GetOK() {
		fmt.Printf("%s was not found\n", args[0])
		return
	}
	fmt.Printf("%s = %s\n", args[0], resp.GetValue())
}

func (r repl) writeRPC(args []string, node *proto.Node) {
	if len(args) < 2 {
		fmt.Println("Write requires a key and a value to write.")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := node.WriteRPC(ctx, &proto.WriteRequest{Key: args[0], Value: args[1], Time: timestamppb.Now()})
	cancel()
	if err != nil {
		fmt.Printf("Write RPC finished with error: %v\n", err)
		return
	}
	if !resp.GetNew() {
		fmt.Printf("Failed to update %s: timestamp too old.\n", args[0])
		return
	}
	fmt.Println("Write OK")
}

func (r repl) listKeysRPC(node *proto.Node) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := node.ListKeysRPC(ctx, &proto.ListRequest{})
	cancel()
	if err != nil {
		fmt.Printf("ListKeys RPC finished with error: %v\n", err)
		return
	}

	keys := ""
	for _, k := range resp.GetKeys() {
		keys += k + ", "
	}
	fmt.Println("Keys found: ", keys)
}

func (r repl) multicast(args []string) {
	if len(args) < 2 {
		fmt.Println("'multicast' requires a key and a value.")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	r.cfg.WriteMulticast(ctx, &proto.WriteRequest{Key: args[0], Value: args[1]})
	cancel()
	fmt.Println("Multicast OK: (server output not synchronized)")
}

func (r repl) qc(args []string) {
	if len(args) < 1 {
		fmt.Println("'qc' requires an operation.")
		return
	}

	switch args[0] {
	case "read":
		r.doReadQC(args[1:])
	case "write":
		r.doWriteQC(args[1:])
	case "list":
		r.doListQC()
	}
}

func (r repl) doReadQC(args []string) {
	if len(args) < 1 {
		fmt.Println("Read requires a key to read.")
		return
	}
	resp := r.readQC(args[0], r.cfg)
	if !resp.GetOK() {
		fmt.Printf("%s was not found\n", args[0])
		return
	}
	fmt.Printf("%s = %s\n", args[0], resp.GetValue())
}

func (r repl) doWriteQC(args []string) {
	if len(args) < 2 {
		fmt.Println("Write requires a key and a value to write.")
		return
	}
	resp := r.writeQC(args[0], args[1], r.cfg)
	if !resp.GetNew() {
		fmt.Printf("Failed to update %s: timestamp too old.\n", args[0])
		return
	}
	fmt.Println("Write OK")
}

func (r repl) doListQC() {

	resp := r.listQC(r.cfg)

	if len(resp.GetKeys()) == 0 {
		fmt.Println("No keys found.")
		return
	}

	keys := ""
	for _, k := range resp.GetKeys() {
		keys += k + ", "
	}
	fmt.Println("Keys found: ", keys)
}

func (r repl) cfgc(args []string) {
	if len(args) < 1 {
		fmt.Println("'cfg' requires a configuration.")
		return
	}
	cfg := r.parseConfiguration(args[0])
	if cfg == nil {
		return
	}

	r.cfg = cfg
}
