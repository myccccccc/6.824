package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	clientID int64
	clientNextCommandNumber int
	leaderID int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientID = nrand()
	ck.clientNextCommandNumber = 0
	ck.leaderID = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	var res string
	for true {
		args := GetArgs{Key:key, ClientID:ck.clientID, ClientCommandNumber:ck.clientNextCommandNumber}
		reply := GetReply{}
		ok := ck.servers[ck.leaderID].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err != "" {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
		ck.clientNextCommandNumber++
		res = reply.Value
		break
	}
	return res
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	for true {
		args := PutAppendArgs{Key:key, Value:value, Op:op, ClientID:ck.clientID, ClientCommandNumber:ck.clientNextCommandNumber}
		reply := GetReply{}
		ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err != "" {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
		ck.clientNextCommandNumber++
		break
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
