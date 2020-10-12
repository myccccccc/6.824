package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	OpType string // "Get" or "Put" or "Append"
	Key string
	Value string // for "Get", this is the return value, for "Put" and "Append" this is what the client want to change
	ClientID int64
	ClientCommandNumber int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	m       map[string]string // the actual storage part for the kv server
	clis    map[int64]*cli

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

// the state machine tracks the latest
// serial number processed for each client, along with the as-
// sociated response. If it receives a command whose serial
// number has already been executed, it responds immedi-
// ately without re-executing the request.
type cli struct {
	ops map[int]Op // ClientCommandNumber 2 Operation
	maxClientCommandNumberPlusOne int // max ClientCommandNumber plus one in ops
	noReplyYet int32 // 1 means the server send an op to raft, but haven't got a reply from raft
	mu         sync.Mutex
	cd         *sync.Cond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	_, ok := kv.clis[args.ClientID]
	if !ok {
		kv.clis[args.ClientID] = new(cli)
		kv.clis[args.ClientID].ops = make(map[int]Op)
		kv.clis[args.ClientID].cd = sync.NewCond(&kv.clis[args.ClientID].mu)
	}

	if (atomic.LoadInt32(&kv.clis[args.ClientID].noReplyYet) == 1) {
		reply.Err = "Time Out"
		reply.Value = ""
		kv.mu.Unlock()
		return
	}

	if args.ClientCommandNumber < kv.clis[args.ClientID].maxClientCommandNumberPlusOne {
		//receives a command whose serial number has already been executed, it responds immediately without re-executing the request.
		reply.Err = ""
		reply.Value = kv.clis[args.ClientID].ops[args.ClientCommandNumber].Value
		DPrintf("%v is giving back %v to client\n", kv.me, kv.clis[args.ClientID].ops[args.ClientCommandNumber])
		kv.mu.Unlock()
		return
	}

	noReplyYetAddr := &kv.clis[args.ClientID].noReplyYet
	cd := kv.clis[args.ClientID].cd
	kv.mu.Unlock()

	op := Op{OpType:"Get", Key:args.Key, Value:"", ClientID:args.ClientID, ClientCommandNumber:args.ClientCommandNumber}
	atomic.StoreInt32(noReplyYetAddr, 1)
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		atomic.StoreInt32(noReplyYetAddr, 0)
		reply.Err = "Not Leader"
		reply.Value = ""
	} else {
		ch := make(chan int)
		go func () {
			defer func () {recover()} ()
			cd.L.Lock()
			cd.Wait()
			ch <- 0
			cd.L.Unlock()
		} ()
		select {
		case <-ch:
			kv.mu.Lock()
			if args.ClientCommandNumber < kv.clis[args.ClientID].maxClientCommandNumberPlusOne {
				//receives a command whose serial number has already been executed, it responds immediately without re-executing the request.
				reply.Err = ""
				reply.Value = kv.clis[args.ClientID].ops[args.ClientCommandNumber].Value
				DPrintf("%v is giving back %v to client\n", kv.me, kv.clis[args.ClientID].ops[args.ClientCommandNumber])
			} else {
				reply.Err = "Try Another Time"
				reply.Value = ""
			}
			kv.mu.Unlock()
		case <-time.After(time.Second):
			reply.Err = "Time Out"
			reply.Value = ""
		}
		close(ch)
		// op = <-ch
		// reply.Err = ""
		// reply.Value = op.Value
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	_, ok := kv.clis[args.ClientID]
	if !ok {
		kv.clis[args.ClientID] = &cli{}
		kv.clis[args.ClientID].ops = make(map[int]Op)
		kv.clis[args.ClientID].cd = sync.NewCond(&kv.clis[args.ClientID].mu)
	}

	if (atomic.LoadInt32(&kv.clis[args.ClientID].noReplyYet) == 1) {
		reply.Err = "Time Out"
		kv.mu.Unlock()
		return
	}

	if args.ClientCommandNumber < kv.clis[args.ClientID].maxClientCommandNumberPlusOne {
		//receives a command whose serial number has already been executed, it responds immediately without re-executing the request.
		reply.Err = ""
		DPrintf("%v is giving back %v to client\n", kv.me, kv.clis[args.ClientID].ops[args.ClientCommandNumber])
		kv.mu.Unlock()
		return
	}

	noReplyYetAddr := &kv.clis[args.ClientID].noReplyYet
	cd := kv.clis[args.ClientID].cd
	kv.mu.Unlock()

	op := Op{OpType:args.Op, Key:args.Key, Value:args.Value, ClientID:args.ClientID, ClientCommandNumber:args.ClientCommandNumber}
	atomic.StoreInt32(noReplyYetAddr, 1)
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		atomic.StoreInt32(noReplyYetAddr, 0)
		reply.Err = "Not Leader"
	} else {
		ch := make(chan int)
		go func () {
			defer func () {recover()} ()
			cd.L.Lock()
			cd.Wait()
			ch <- 0
			cd.L.Unlock()
		} ()
		select {
		case <-ch:
			kv.mu.Lock()
			if args.ClientCommandNumber < kv.clis[args.ClientID].maxClientCommandNumberPlusOne {
				//receives a command whose serial number has already been executed, it responds immediately without re-executing the request.
				reply.Err = ""
				DPrintf("%v is giving back %v to client\n", kv.me, kv.clis[args.ClientID].ops[args.ClientCommandNumber])
			} else {
				reply.Err = "Try Another Time"
			}
			kv.mu.Unlock()
		case <-time.After(time.Second):
			reply.Err = "Time Out"
		}
		close(ch)
		// op = <-ch
		// reply.Err = ""
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.m = make(map[string]string)
	kv.clis = make(map[int64]*cli)

	go kv.goapply()

	return kv
}

func (kv *KVServer) goapply() {
	for true {
		ae := <-kv.applyCh
		if ae.Command == nil {
			continue
		}

		kv.mu.Lock()
		op := ae.Command.(Op)
		_, ok := kv.clis[op.ClientID]
		if !ok {
			kv.clis[op.ClientID] = new(cli)
			kv.clis[op.ClientID].ops = make(map[int]Op)
			kv.clis[op.ClientID].cd = sync.NewCond(&kv.clis[op.ClientID].mu)
		}
		if e, ok := kv.clis[op.ClientID].ops[op.ClientCommandNumber]; !ok {
			kv.apply(&op)
			kv.clis[op.ClientID].maxClientCommandNumberPlusOne++
			DPrintf("%v has applied %v\n", kv.me, op)
		} else {
			DPrintf("%v %v is the same as %v\n", kv.me, op, e)
			op = e
		}

		atomic.StoreInt32(&kv.clis[op.ClientID].noReplyYet, 0)
		kv.clis[op.ClientID].ops[op.ClientCommandNumber] = op
		kv.clis[op.ClientID].cd.Broadcast()
		kv.mu.Unlock()
	}
}

func (kv *KVServer) apply(op *Op) {
	if op.OpType == "Get" {
		value, ok := kv.m[op.Key]
		if ok {
			op.Value = value
		} else {
			op.Value = ""
		}
	}
	if op.OpType == "Put" {
		kv.m[op.Key] = op.Value
	}
	if op.OpType == "Append"{
		_, ok := kv.m[op.Key]
		if ok {
			kv.m[op.Key] += op.Value
		} else {
			kv.m[op.Key] = op.Value
		}
	}
}