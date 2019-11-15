package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.

//

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
/*
当每个raft节点意识到提交了连续的日志条目时，节点应该通过传递给Make()的applyCh向同一服务器上的服务(或测试人员)发送ApplyMsg。 ##
*/
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Index int //日志索引
	Term  int //日志创建的任期号
}

//
// A Go object implementing a single Raft peer.
//
/*
实现Raft的一个Go对象
*/
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state         string    //表示状态，分别有Follower,Candidate,Leader三种状态
	voteCount     int       //得票数
	chanHeartbeat chan bool //心跳机制，用来建立权限联系以及阻止其他选举的产生
	chanGrantVote chan bool //判断该服务器是否投过票
	chanIsLeader  chan bool //判断是否成为了领导人

	//persistent state on all servers:
	currentTerm int        //服务器看到的最新任期(第一次启动时初始化为0，单调递增)。
	votedFor    int        //投票给了哪个服务器，也就是peer中的位置
	log         []LogEntry //日志的条目

	//volatile state on all servers
	commitIndex int //已知的最大的已经被提交的日志索引值
	lastApplied int //最后别应用到状态机的日志索引值

	//volatile state on leaders
	nextIndex  []int //对每一个服务器，需要发送给它的下一个日志条目索引值，initial 为leader的最后一次log索引加一
	matchIndex []int //对每一个服务器，已经复制的日志的最高索引值

}

// return currentTerm and whether this server
// believes it is the leader.

/*
返回当前任期以及这个服务器是否认为自己是一个leader
*/
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = (rf.state == "Leader")
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
/*
将Raft的持久状态保存到稳定的存储中，以便在崩溃和重新启动后检索。
有关什么应该是持久性的描述，请参见本文的图2。
*/
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int //候选人任期号
	CandidateId  int //候选人的id，即在peers中的位置
	LastLogTerm  int //候选人最后日志条目的任期
	LastLogIndex int //候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  //当前的任期号
	VoteGranted bool //候选人赢得了这个服务器的选票时为真
}
type AppendEntriesArgs struct {
	Term int
}
type AppendEntriesReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.VoteGranted = false

	mayGrantVote := false
	term := rf.log[len(rf.log)-1].Term
	index := rf.log[len(rf.log)-1].Index
	if term < args.LastLogTerm || (term == args.LastLogTerm && args.LastLogIndex >= index) {
		mayGrantVote = true
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "Follower"
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && mayGrantVote {
		rf.chanGrantVote <- true
		rf.state = "Follower"
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
	return
}
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock() // defer的先后次序
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.chanHeartbeat <- true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "Follower"
		rf.votedFor = -1
	}
	reply.Term = args.Term
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) handleRequestReply(reply *RequestVoteReply) {

	if reply.Term < rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = "Follower"
		rf.votedFor = -1
		rf.persist()
	}
	if rf.state == "Candidate" && reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			rf.state = "Leader"
			rf.chanIsLeader <- true
		}
	}
}
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//return ok

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.state != "Candidate" {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}
		rf.handleRequestReply(reply)
		return ok
	}
	return ok
}
func (rf *Raft) handleAppendEntriesReply(reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = "Follower"
		rf.votedFor = -1
		rf.persist()
		return
	}
}
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != "Leader" {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}
		rf.handleAppendEntriesReply(reply)
		return ok
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

/*
使用Raft的服务器(例如k/v服务器)希望启动协议，将下一个命令附加到Raft日志中。如果该服务器不是leader，则返回false。否则启动协议并立即返回。
没有人能保证这个命令会被放在Raft上，因为领导者可能会在选举中失败或失败。(##)
第一个返回值是命令提交时将出现的索引。第二个返回值是当前项。如果此服务器认为自己是leader，则第三个返回值为true。(##)
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) { // ##这里并没有用到command
	/*index := -1
	term := -1
	isLeader := true


	return index, term, isLeader*/

	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := (rf.state == "Leader")

	return index, term, isLeader
}
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == "Leader" {
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			go func(i int, args AppendEntriesArgs) {
				var reply AppendEntriesReply
				rf.sendAppendEntries(i, args, &reply)
			}(i, args)
		}
	}
}
func (rf *Raft) broadcastRequestVote() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	defer rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == "Candidate" {
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, args, &reply)
			}(i)
		}
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

/*
Make的作用：创建一个Raft服务器。
所有服务器的端口号都在peers[]中。本服务器的端口号是peers[me]。所有服务器的peer数组都有相同的顺序。persister 是服务器存储 persistent 状态的地方（##），
并且初始化的时候存储最近被保存的状态，如果有的话（##）。
applyCh是测试人员或服务期望Raft发送ApplyMsg消息的通道。
Make()必须快速返回，因此它应该为任何长时间运行的工作启动goroutines(##)。

*/
//## 当一个服务器一段时间没收到心跳消息后，便发起一次选举，根据不同的状态产生不同的操作 这个操作在哪里？
func Make(peers []*labrpc.ClientEnd, me int, // []*是什么意思 ClientEnd指针的数组
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	// 期初应该都是 Follower
	rf.state = "Follower"
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.currentTerm = 0
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanIsLeader = make(chan bool, 100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go func() { //##
		for {
			/*
				处于 Follower 状态的节点在一个随机的超时时间 (称之为 Election timeout，注意每次都要随机选择一个超时时间，这个超时时间通常为 150-300 毫秒，
				我在实验中设置的是 300+ms) 内没有收到投票或者日志复制和心跳包的 RPC，则会变成 Candidate 状态。
			*/
			if rf.state == "Follower" {
				select { //##
				case <-rf.chanHeartbeat: // 收到心跳包
				case <-rf.chanGrantVote: // 收到日志赋值的RPC
				case <-time.After(time.Duration(rand.Int63()%150+300) * time.Millisecond):
					rf.state = "Candidate"
				}
			}
			/*

			 */
			if rf.state == "Leader" {
				rf.broadcastAppendEntries()
				time.Sleep(50 * time.Millisecond)
			}
			/*
				处于 Candidate 状态的节点会马上开始选举投票。它先投自己一票，然后向其他节点发送投票，这个请求称之为 Request Vote RPC。
				如果获得了过半的节点投票，则转成 Leader 状态。如果投票给了其他节点或者发现了更新任期 (Term) 的指令 (如更新任期的选举投票和日志复制指令)，则转为 Follower 状态。
				如果选举超时没有得到过半投票，则保持 Candidate 状态开始一个新一轮选举投票。
			*/
			if rf.state == "Candidate" {
				rf.mu.Lock() // 这里为何要加锁？
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.persist()
				rf.mu.Unlock()
				go rf.broadcastRequestVote()
				select {
				case <-time.After(time.Duration(rand.Int63()%150+500) * time.Millisecond):
				case <-rf.chanHeartbeat:
					rf.state = "Follower"
				case <-rf.chanIsLeader:
					rf.mu.Lock()
					rf.state = "Leader"
					rf.mu.Unlock()
				}
			}
		}
	}()

	return rf
}
