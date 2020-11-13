package raft

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const (
	follower  = iota
	candidate = iota
	leader    = iota
)

const heartbeats = 100

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int

	// Your data here (2A, 2B, 2C).
	term       int
	timer      *time.Timer
	timeout    time.Duration
	state      int
	appendCh   chan bool
	voteCh     chan bool
	applyMsgCh chan ApplyMsg
	voteCount  int
	votedFor   int
	log        []LogEntry
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()

	term = rf.term
	if rf.state == leader {
		isleader = true
	} else {
		isleader = false
	}

	rf.mu.Unlock()

	return term, isleader
}

func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.term)
	d.Decode(&rf.log)
	if data == nil || len(data) < 1 {
		return
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.VoteGranted = false

	if args.Term < rf.term {
		reply.Term = rf.term
		return
	} else if args.Term > rf.term {
		rf.term = args.Term
		rf.state = follower
		rf.votedFor = -1
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
	reply.Term = rf.term
	go func() {
		rf.voteCh <- true
	}()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = true
	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		return
	} else if args.Term > rf.term {
		rf.term = args.Term
		rf.state = follower
		rf.votedFor = -1
	}
	go func() {
		rf.appendCh <- true
	}()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) readSnapshot(data []byte) {
	rf.readPersist(rf.persister.ReadRaftState())
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var LastTerm int

	d.Decode(&LastTerm)
	rf.log = append(rf.log, LogEntry{Term: LastTerm})
	msg := ApplyMsg{UseSnapshot: true, Snapshot: data}
	rf.applyMsgCh <- msg
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = follower
	rf.term = 0
	rf.votedFor = -1
	rf.voteCh = make(chan bool)
	rf.appendCh = make(chan bool)
	rf.applyMsgCh = applyCh

	electionTimeout := (heartbeats + rand.Intn(heartbeats)) * 5
	rf.timeout = time.Duration(electionTimeout) * time.Millisecond
	rf.timer = time.NewTimer(rf.timeout)

	go rf.selectState()

	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	return rf
}

func (rf *Raft) selectState() {
	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		electionTimeout := (heartbeats + rand.Intn(heartbeats)) * 5
		rf.timeout = time.Duration(electionTimeout) * time.Millisecond

		switch state {
		case follower:
			select {
			case <-rf.appendCh:
				rf.timer.Reset(rf.timeout)
			case <-rf.voteCh:
				rf.timer.Reset(rf.timeout)
			case <-rf.timer.C:
				rf.mu.Lock()
				rf.state = candidate
				rf.mu.Unlock()
				rf.start()
			}
		case candidate:
			select {
			case <-rf.appendCh:
				rf.timer.Reset(rf.timeout)
				rf.mu.Lock()
				rf.state = follower
				rf.votedFor = -1
				rf.mu.Unlock()
			case <-rf.timer.C:
				rf.start()
			default:
				rf.mu.Lock()
				if rf.voteCount > len(rf.peers)/2 {
					rf.state = leader
				}
				rf.mu.Unlock()
			}
		case leader:
			for peer, _ := range rf.peers {
				if peer != rf.me {
					go func(peer int) {
						args := AppendEntriesArgs{}
						rf.mu.Lock()
						args.Term = rf.term
						args.LeaderId = rf.me
						rf.mu.Unlock()
						reply := AppendEntriesReply{}
						if rf.sendAppendEntries(peer, &args, &reply) {
							rf.mu.Lock()
							if reply.Term > rf.term {
								rf.term = reply.Term
								rf.state = follower
								rf.votedFor = -1
							}
							rf.mu.Unlock()
						}
					}(peer)
				}
			}
			time.Sleep(heartbeats)
		}
	}
}

func (rf *Raft) start() {
	rf.mu.Lock()
	rf.term += 1
	rf.votedFor = rf.me
	rf.timer.Reset(rf.timeout)
	rf.voteCount = 1
	rf.mu.Unlock()
	for peer, _ := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				rf.mu.Lock()
				args := RequestVoteArgs{}
				args.Term = rf.term
				args.CandidateId = rf.me
				rf.mu.Unlock()
				reply := RequestVoteReply{}
				if rf.sendRequestVote(peer, &args, &reply) {
					rf.mu.Lock()
					if reply.VoteGranted {
						rf.voteCount += 1
					} else if reply.Term > rf.term {
						rf.term = reply.Term
						rf.state = follower
						rf.votedFor = -1
					}
					rf.mu.Unlock()
				} else {
				}
			}(peer)
		}
	}
}
