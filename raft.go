package hraft

import (
	"github.com/hlccd/hraft/protocol"
	"log"
	"net"
	"sync"
	"time"
)

type Raft struct {
	id       int64    //该结点id号,纳秒时间戳
	vote     int64    //上次投票支持的节点的id
	master   net.Conn //核心集群中的master结点连接
	cores    []*core  //该结点集群的其他结点地址
	lastKeep int64    //最后一次接收心跳的时间戳
	inVote   bool     //在选举中?
	point    int
	read     chan string
	write    chan string
	sync.RWMutex
}

func NewRaft(point int, clusterAddr string) (raft *Raft) {
	raft = &Raft{
		id:       time.Now().UnixNano(),
		vote:     0,
		master:   nil,
		cores:    make([]*core, 0, 0),
		lastKeep: time.Now().UnixNano(),
		point:    point,
		read:     make(chan string, 1024),
		write:    make(chan string, 1024),
	}
	power := protocol.Master
	if len(clusterAddr) != 0 {
		if !raft.accessCluster(clusterAddr, getAddr(point)) {
			return nil
		}
		power = protocol.Endpoint
	} else {
		go raft.clusterKeep()
	}
	go raft.listen(point)
	go raft.msg()
	raft.cores = append(raft.cores, newCore(raft.id, getAddr(point), power))
	return raft
}

// 开启端口监听信息
func (raft *Raft) listen(point int) {
	l, err := net.Listen("tcp", getAddr(point))
	log.Println("正在监听端口:", point)
	if err != nil {
		log.Println("err:", err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("err:", err)
		}
		msg, err := protocol.GetMessage(conn)
		if err == nil && msg != nil {
			// 分流处理信息
			go raft.shunt(msg, conn)
		} else {
			conn.Close()
		}
	}
}

func (raft *Raft) IsMaster() bool {
	if raft == nil {
		return false
	}
	return raft.master == nil
}

func (raft *Raft) Get() (s string) {
	if raft == nil {
		return ""
	}
	select {
	case <-time.After(time.Second):
		return ""
	case s = <-raft.read:
		log.Println("接收到信息:", s)
		return s
	}
}
func (raft *Raft) Put(s string) bool {
	if raft == nil {
		return false
	}
	ch := make(chan bool)
	go func() {
		raft.write <- s
		ch <- true
	}()
	select {
	case <-time.After(time.Second):
		return false
	case <-ch:
		return true
	}
}

// 将收到的信息推送到整个集群
func (raft *Raft) msg() {
	var s string
	for {
		s = <-raft.write
		if raft.IsMaster() {
			log.Println("正在向集群推送信息: ", s)
			for _, c := range raft.cores {
				protocol.SendMessage(c.GetConn(), protocol.Master, protocol.Endpoint, protocol.MSG, s)
			}
			go raft.received(s)
		} else {
			log.Println("正在向主节点推送信息: ", s, raft.master.RemoteAddr().String())
			protocol.SendMessage(raft.master, protocol.Endpoint, protocol.Master, protocol.MSG, s)
		}
	}
}
