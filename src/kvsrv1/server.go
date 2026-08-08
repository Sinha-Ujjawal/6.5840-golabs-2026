package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...any) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type value struct {
	val string
	ver rpc.Tversion
}

type KVServer struct {
	mu      sync.RWMutex
	mapping map[string]value
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.mu = sync.RWMutex{}
	kv.mapping = make(map[string]value)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	DPrintf("Mapping: %+v, Get(args: %+v)\n", kv.mapping, args)
	reply.Err = rpc.OK
	value, ok := kv.mapping[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Value = value.val
		reply.Version = value.ver
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Mapping: %+v, Put(args: %+v)\n", kv.mapping, args)
	reply.Err = rpc.OK
	var ok bool
	var oldValue value
	oldValue, ok = kv.mapping[args.Key]
	if !ok && args.Version != 0 {
		reply.Err = rpc.ErrNoKey
		return
	} else if oldValue.ver != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}
	oldValue.val = args.Value
	oldValue.ver = args.Version + 1
	kv.mapping[args.Key] = oldValue
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
