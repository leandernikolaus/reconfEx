package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"examplestorage/proto"

	"github.com/relab/gorums"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func startServer(address string) (*gorums.Server, string) {
	// listen on given address
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen on '%s': %v\n", address, err)
	}

	// init server implementation
	storage := newStorageServer()
	storage.logger = log.New(os.Stderr, fmt.Sprintf("%s: ", lis.Addr()), log.Ltime|log.Lmicroseconds|log.Lmsgprefix)

	// create Gorums server
	srv := gorums.NewServer()
	// register server implementation with Gorums server
	proto.RegisterStorageServer(srv, storage)
	// handle requests on listener
	go func() {
		err := srv.Serve(lis)
		if err != nil {
			storage.logger.Fatalf("Server error: %v\n", err)
		}
	}()

	return srv, lis.Addr().String()
}

func runServer(address string) {
	// catch signals in order to shut down gracefully
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	srv, addr := startServer(address)

	log.Printf("Started storage server on %s\n", addr)

	<-signals
	// shutdown Gorums server
	srv.Stop()
}

type state struct {
	Value string
	Time  time.Time
}

// storageServer is an implementation of proto.Storage
type storageServer struct {
	storage map[string]state
	configs []*proto.Config
	mut     sync.RWMutex
	logger  *log.Logger
}

func newStorageServer() *storageServer {
	return &storageServer{
		storage: make(map[string]state),
		configs: make([]*proto.Config, 0, 1),
	}
}

// ReadRPC is an RPC handler
func (s *storageServer) ReadRPC(_ gorums.ServerCtx, req *proto.ReadRequest) (resp *proto.ReadResponse, err error) {
	return s.Read(req)
}

// WriteRPC is an RPC handler
func (s *storageServer) WriteRPC(_ gorums.ServerCtx, req *proto.WriteRequest) (resp *proto.WriteResponse, err error) {
	return s.Write(req)
}

// ReadQC is an RPC handler for a quorum call
func (s *storageServer) ReadQC(_ gorums.ServerCtx, req *proto.ReadRequest) (resp *proto.ReadResponse, err error) {
	return s.Read(req)
}

// WriteQC is an RPC handler for a quorum call
func (s *storageServer) WriteQC(_ gorums.ServerCtx, req *proto.WriteRequest) (resp *proto.WriteResponse, err error) {
	return s.Write(req)
}

func (s *storageServer) ListKeysRPC(_ gorums.ServerCtx, req *proto.ListRequest) (*proto.ListResponse, error) {
	return s.ListKeys(req)
}

func (s *storageServer) ListKeysQC(_ gorums.ServerCtx, req *proto.ListRequest) (*proto.ListResponse, error) {
	return s.ListKeys(req)
}

func (s *storageServer) WriteConfigQC(_ gorums.ServerCtx, req *proto.Config) (resp *proto.WriteResponse, err error) {
	return s.WriteConfig(req)
}

func (s *storageServer) WriteMulticast(_ gorums.ServerCtx, req *proto.WriteRequest) {
	_, err := s.Write(req)
	if err != nil {
		s.logger.Printf("Write error: %v", err)
	}
}

// Read reads a value from storage
func (s *storageServer) Read(req *proto.ReadRequest) (*proto.ReadResponse, error) {
	s.logger.Printf("Read '%s'\n", req.GetKey())
	s.mut.RLock()
	defer s.mut.RUnlock()
	state, ok := s.storage[req.GetKey()]
	if !ok {
		return &proto.ReadResponse{OK: false, Config: s.configs}, nil
	}
	return &proto.ReadResponse{OK: true, Value: state.Value, Time: timestamppb.New(state.Time), Config: s.configs}, nil
}

// Write writes a new value to storage if it is newer than the old value
func (s *storageServer) Write(req *proto.WriteRequest) (*proto.WriteResponse, error) {
	s.logger.Printf("Write '%s' = '%s'\n", req.GetKey(), req.GetValue())
	s.mut.Lock()
	defer s.mut.Unlock()
	oldState, ok := s.storage[req.GetKey()]
	if ok && oldState.Time.After(req.GetTime().AsTime()) {
		return &proto.WriteResponse{New: false, Config: s.configs}, nil
	}
	s.storage[req.GetKey()] = state{Value: req.GetValue(), Time: req.GetTime().AsTime()}
	return &proto.WriteResponse{New: true, Config: s.configs}, nil
}

func (s *storageServer) WriteConfig(req *proto.Config) (*proto.WriteResponse, error) {
	s.logger.Printf("Config '%s', started: '%t'\n", req.GetAdds(), req.GetStarted())
	s.mut.Lock()
	defer s.mut.Unlock()

	if len(s.configs) > 0 && s.configs[0].GetStarted() && s.configs[0].GetTime().AsTime().After(req.GetTime().AsTime()) {
		// An oldd configuration, ignore
		return &proto.WriteResponse{New: false, Config: s.configs}, nil
	}

	if req.GetStarted() {
		confs := make([]*proto.Config, len(s.configs))
		confs = append(confs, req)
		for _, c := range s.configs {
			if req.GetTime().AsTime().Before(c.Time.AsTime()) {
				confs = append(confs, c)
			}
		}
		s.configs = confs
		return &proto.WriteResponse{New: true, Config: s.configs}, nil
	}

	s.configs = append(s.configs, req)
	return &proto.WriteResponse{New: true, Config: s.configs}, nil
}

// Write writes a new value to storage if it is newer than the old value
func (s *storageServer) ListKeys(req *proto.ListRequest) (*proto.ListResponse, error) {
	s.logger.Printf("List request")
	s.mut.Lock()
	defer s.mut.Unlock()
	keys := make([]string, 0, len(s.storage))

	for k := range s.storage {
		keys = append(keys, k)
	}

	return &proto.ListResponse{Keys: keys}, nil
}
