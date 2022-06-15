package main

import (
	"examplestorage/proto"
)

type qspec struct {
	cfgSize int
}

// ReadQCQF is the quorum function for the ReadQC
// ordered quorum call method. The in parameter is the request object
// supplied to the ReadQC method at call time, and may or may not
// be used by the quorum function. If the in parameter is not needed
// you should implement your quorum function with '_ *ReadRequest'.
func (q qspec) ReadQCQF(_ *proto.ReadRequest, replies map[uint32]*proto.ReadResponse) (*proto.ReadResponse, bool) {
	// wait until at least half of the replicas have responded
	if len(replies) <= q.cfgSize/2 {
		return nil, false
	}
	// return the value with the most recent timestamp
	return newestValue(replies), true
}

// WriteQCQF is the quorum function for the WriteQC
// ordered quorum call method. The in parameter is the request object
// supplied to the WriteQC method at call time, and may or may not
// be used by the quorum function. If the in parameter is not needed
// you should implement your quorum function with '_ *WriteRequest'.
func (q qspec) WriteQCQF(in *proto.WriteRequest, replies map[uint32]*proto.WriteResponse) (*proto.WriteResponse, bool) {
	// wait until at least half of the replicas have responded and have updated their value
	if numUpdated(replies) <= q.cfgSize/2 {
		// if all replicas have responded, there must have been another write before ours
		// that had a newer timestamp
		if len(replies) == q.cfgSize {
			return &proto.WriteResponse{New: false, Config: writeCombineConfs(replies)}, true
		}
		return nil, false
	}
	return &proto.WriteResponse{New: true, Config: writeCombineConfs(replies)}, true
}

func (q qspec) ListKeysQCQF(in *proto.ListRequest, replies map[uint32]*proto.ListResponse) (*proto.ListResponse, bool) {
	if len(replies) <= q.cfgSize/2 {
		return nil, false
	}
	var keys map[string]bool
	for _, resp := range replies {
		if len(keys) == 0 {
			keys = make(map[string]bool, len(resp.GetKeys()))
		}
		for _, k := range resp.GetKeys() {
			keys[k] = true
		}
	}
	allkeys := make([]string, 0, len(keys))
	for k := range keys {
		allkeys = append(allkeys, k)
	}
	return &proto.ListResponse{Keys: allkeys, Config: listCombineConfs(replies)}, true
}

func (q qspec) WriteConfigQCQF(in *proto.Config, replies map[uint32]*proto.WriteResponse) (*proto.WriteResponse, bool) {
	if numUpdated(replies) <= q.cfgSize/2 {
		// if all replicas have responded, there must have been another write before ours
		// that had a newer timestamp
		if len(replies) == q.cfgSize {
			return &proto.WriteResponse{New: false, Config: writeCombineConfs(replies)}, true
		}
		return nil, false
	}
	return &proto.WriteResponse{New: true, Config: writeCombineConfs(replies)}, true
}

// newestValue returns the reply that had the most recent timestamp
func newestValue(values map[uint32]*proto.ReadResponse) *proto.ReadResponse {
	if len(values) < 1 {
		return nil
	}
	var newest *proto.ReadResponse
	for _, v := range values {
		if v.GetTime().AsTime().After(newest.GetTime().AsTime()) {
			newest = v
		}
	}
	newest.Config = readCombineConfs(values)
	return newest
}

// numUpdated returns the number of replicas that updated their value
func numUpdated(replies map[uint32]*proto.WriteResponse) int {
	count := 0
	for _, r := range replies {
		if r.GetNew() {
			count++
		}
	}
	return count
}

func listCombineConfs(replies map[uint32]*proto.ListResponse) []*proto.Config {
	configlists := make([][]*proto.Config, len(replies))
	for _, wr := range replies {
		configlists = append(configlists, wr.GetConfig())
	}
	return combineConfs(configlists)
}

func writeCombineConfs(replies map[uint32]*proto.WriteResponse) []*proto.Config {
	configlists := make([][]*proto.Config, len(replies))
	for _, wr := range replies {
		configlists = append(configlists, wr.GetConfig())
	}
	return combineConfs(configlists)
}

func readCombineConfs(replies map[uint32]*proto.ReadResponse) []*proto.Config {
	configlists := make([][]*proto.Config, len(replies))
	for _, rr := range replies {
		configlists = append(configlists, rr.GetConfig())
	}
	return combineConfs(configlists)
}

func combineConfs(configlists [][]*proto.Config) []*proto.Config {
	if len(configlists) == 0 {
		return nil
	}
	confs := make(map[struct {
		int64
		int32
	}]*proto.Config, len(configlists[0]))
	for _, list := range configlists {
		for _, c := range list {
			confs[struct {
				int64
				int32
			}{c.GetTime().Seconds, c.GetTime().Nanos}] = c
		}
	}

	list := make([]*proto.Config, 0, len(confs))
	for _, c := range confs {
		list = append(list, c)
	}
	return list
}
