package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"examplestorage/proto"

	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type client struct {
	mgr  *proto.Manager
	cfg  *proto.Configuration
	pcfg *proto.Config
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

	pcfg := &proto.Config{Adds: "0-" + fmt.Sprint(len(addresses)), Time: &timestamppb.Timestamp{Seconds: 0, Nanos: 0}}

	return &client{
		mgr:  mgr,
		cfg:  cfg,
		pcfg: pcfg,
	}
}

// find config with minimal timestamp
// return key
func getMin(configs map[string]*proto.Config) string {
	if len(configs) == 0 {
		return ""
	}

	var min string

	for s, config := range configs {
		if min == "" || config.Time.AsTime().After(configs[min].Time.AsTime()) {
			min = s
		}
	}
	return min
}

func TimeBefore(a, b *timestamppb.Timestamp) bool {
	return a.AsTime().Before(b.AsTime())
}

func (c *client) read(key string) *proto.ReadResponse {
	cfgs := map[string]*proto.Config{c.pcfg.Time.String(): c.pcfg}
	resp := &proto.ReadResponse{Time: &timestamppb.Timestamp{Seconds: 0, Nanos: 0}}

	for s := getMin(cfgs); s != ""; {
		cfg := c.parseConfiguration(cfgs[s].Adds)
		rresp := c.readQC(key, cfg)

		// remember Value, if it has larger Time
		if TimeBefore(resp.GetTime(), rresp.GetTime()) {
			resp = rresp
		}

		// add configurations to map
		for _, cc := range rresp.Config {
			if TimeBefore(cfgs[s].GetTime(), cc.GetTime()) {
				if cc.GetStarted() {
					cfgs = make(map[string]*proto.Config, 1)
					c.pcfg = cc
				}
				cfgs[cc.GetTime().String()] = cc
			}
		}

		delete(cfgs, s)
	}
	return resp
}

func (client) readQC(key string, cfg *proto.Configuration) *proto.ReadResponse {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cfg.ReadQC(ctx, &proto.ReadRequest{Key: key})
	cancel()
	if err != nil {
		fmt.Printf("Read RPC finished with error: %v\n", err)
		return nil
	}
	return resp
}

func (client) writeQC(key, value string, cfg *proto.Configuration) *proto.WriteResponse {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cfg.WriteQC(ctx, &proto.WriteRequest{Key: key, Value: value, Time: timestamppb.Now()})
	cancel()
	if err != nil {
		fmt.Printf("Write RPC finished with error: %v\n", err)
		return nil
	}
	return resp
}

func (client) listQC(cfg *proto.Configuration) *proto.ListResponse {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cfg.ListKeysQC(ctx, &proto.ListRequest{})
	cancel()
	if err != nil {
		fmt.Printf("ListKeys RPC finished with error: %v\n", err)
		return nil
	}
	return resp
}

func (client) writeConfigQC(conf *proto.Config, cfg *proto.Configuration) *proto.WriteResponse {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cfg.WriteConfigQC(ctx, conf)
	cancel()
	if err != nil {
		fmt.Printf("ListKeys RPC finished with error: %v\n", err)
		return nil
	}
	return resp
}

func (c *client) reconf(newAdds string) {

	goalProtoConf := &proto.Config{Adds: newAdds, Started: false, Time: timestamppb.Now()}
	goalCfg := c.parseConfiguration(newAdds)
	// stop old and inform about new config
	c.writeConfigQC(goalProtoConf, c.cfg)

	// transfer state
	list := c.listQC(c.cfg)
	for _, key := range list.GetKeys() {
		value := c.readQC(key, c.cfg)
		if value.GetOK() {
			c.writeQC(key, value.GetValue(), goalCfg)
		}
	}

	// start new config
	goalProtoConf.Started = true
	c.writeConfigQC(goalProtoConf, goalCfg)
	c.cfg = goalCfg
}

func (c client) parseConfiguration(cfgStr string) (cfg *proto.Configuration) {
	// configuration using range syntax
	if i := strings.Index(cfgStr, ":"); i > -1 {
		var start, stop int
		var err error
		numNodes := c.mgr.Size()
		if i == 0 {
			start = 0
		} else {
			start, err = strconv.Atoi(cfgStr[:i])
			if err != nil {
				fmt.Printf("Failed to parse configuration: %v\n", err)
				return nil
			}
		}
		if i == len(cfgStr)-1 {
			stop = numNodes
		} else {
			stop, err = strconv.Atoi(cfgStr[i+1:])
			if err != nil {
				fmt.Printf("Failed to parse configuration: %v\n", err)
				return nil
			}
		}
		if start >= stop || start < 0 || stop >= numNodes {
			fmt.Println("Invalid configuration.")
			return nil
		}
		nodes := make([]string, 0)
		for _, node := range c.mgr.Nodes()[start:stop] {
			nodes = append(nodes, node.Address())
		}
		cfg, err = c.mgr.NewConfiguration(&qspec{cfgSize: stop - start}, gorums.WithNodeList(nodes))
		if err != nil {
			fmt.Printf("Failed to create configuration: %v\n", err)
			return nil
		}
		return cfg
	}
	// configuration using list of indices
	if indices := strings.Split(cfgStr, ","); len(indices) > 0 {
		selectedNodes := make([]string, 0, len(indices))
		nodes := c.mgr.Nodes()
		for _, index := range indices {
			i, err := strconv.Atoi(index)
			if err != nil {
				fmt.Printf("Failed to parse configuration: %v\n", err)
				return nil
			}
			if i < 0 || i >= len(nodes) {
				fmt.Println("Invalid configuration.")
				return nil
			}
			selectedNodes = append(selectedNodes, nodes[i].Address())
		}
		cfg, err := c.mgr.NewConfiguration(&qspec{cfgSize: len(selectedNodes)}, gorums.WithNodeList(selectedNodes))
		if err != nil {
			fmt.Printf("Failed to create configuration: %v\n", err)
			return nil
		}
		return cfg
	}
	fmt.Println("Invalid configuration.")
	return nil
}
