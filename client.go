package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"examplestorage/proto"

	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type client struct {
	mgr *proto.Manager
	cfg *proto.Configuration
}

func newClient(addresses []string) *client {
	if len(addresses) < 1 {
		log.Fatalln("No addresses provided!")
	}

	// init gorums manager
	mgr := proto.NewManager(
		gorums.WithDialTimeout(1*time.Second),
		gorums.WithGrpcDialOptions(
			grpc.WithBlock(), // block until connections are made
			grpc.WithTransportCredentials(insecure.NewCredentials()), // disable TLS
		),
	)
	// create configuration containing all nodes
	cfg, err := mgr.NewConfiguration(&qspec{cfgSize: len(addresses)}, gorums.WithNodeList(addresses))
	if err != nil {
		log.Fatal(err)
	}

	return &client{
		mgr: mgr,
		cfg: cfg,
	}
}

func (client) readRPC(args []string, node *proto.Node) {
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

func (client) writeRPC(args []string, node *proto.Node) {
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

func (client) listKeysRPC(node *proto.Node) {
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

func (client) readQC(args []string, cfg *proto.Configuration) {
	if len(args) < 1 {
		fmt.Println("Read requires a key to read.")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cfg.ReadQC(ctx, &proto.ReadRequest{Key: args[0]})
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

func (client) writeQC(args []string, cfg *proto.Configuration) {
	if len(args) < 2 {
		fmt.Println("Write requires a key and a value to write.")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cfg.WriteQC(ctx, &proto.WriteRequest{Key: args[0], Value: args[1], Time: timestamppb.Now()})
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

func (client) listQC(cfg *proto.Configuration) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cfg.ListKeysQC(ctx, &proto.ListRequest{})
	cancel()
	if err != nil {
		fmt.Printf("ListKeys RPC finished with error: %v\n", err)
		return
	}

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
