package hraft

import (
	"github.com/hlccd/hraft/protocol"
	"log"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

func (raft *Raft) voteMaster() {
	raft.lastKeep = 0
	log.Println("发起选举")
	rand.Seed(time.Now().UnixNano() * raft.id)
	time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
	raft.Lock()
	defer raft.Unlock()
	if atomic.CompareAndSwapInt64(&raft.vote, 0, raft.id) {
		raft.inVote = true
		raft.master = nil
		defer func() {
			raft.inVote = false
			raft.vote = 0
		}()
		//随机等待后master结点选举开始
		//选举过程采用先到先得,率先获得最高票者为master,票数相同则id更大的为master
		wg := sync.WaitGroup{}
		m := make(map[int64]int64)
		m[raft.id] = 1
		log.Println("我参选")
		voteList := make([]*core, 0, len(raft.cores)-1)
		cancel := false
		for i := range raft.cores {
			if raft.cores[i].Id == raft.id || raft.cores[i].Power == protocol.Master {
				continue
			}
			wg.Add(1)
			go func(c *core) {
				conn, err := net.Dial("tcp", c.Addr)
				c.SetVote(0)
				if err == nil {
					protocol.SendMessage(conn, protocol.Master, protocol.Endpoint, protocol.VOTE, i64s(raft.id))
					msg, err := protocol.WaitMessage(conn)
					if err != nil || msg == nil {
						//链接失败
						c.SetConn(nil)
					} else if msg.GetOperation() == protocol.ACK {
						//链接成功,获取对方支持结果
						c.SetVote(s64(msg.GetContent()))
						log.Printf("节点 %s 支持节点 %s \n", c.Addr, msg.GetContent())
						if s64(msg.GetContent()) == raft.id {
							c.SetConn(conn)
							voteList = append(voteList, c)
						}
					} else if msg.GetOperation() == protocol.KEEP {
						cancel = true
					}
				}
				m[c.GetVote()]++
				wg.Done()
			}(raft.cores[i])
		}
		wg.Wait()
		if cancel {
			log.Println("主节点仍存在,选举取消")
			return
		}
		//投票计算
		log.Println("投票结束,开始计票")
		victor := raft.id
		poll := m[raft.id]
		for k, v := range m {
			log.Printf("节点 %d 的票: %d\n", k, v)
			if v > poll {
				poll = v
				victor = k
			} else if v == poll && k > victor {
				// 票数相同时,id更大的节点优胜
				poll = v
				victor = k
			}
		}
		if int(m[0]) >= len(raft.cores)/2 {
			victor = 0
		}
		log.Println("计票完成,胜选的是:", victor)
		//投票完成
		if victor == raft.id {
			log.Println("我胜选了")
			raft.master = nil
			//通知所有链接成功的点我已胜选
			c := newCore(raft.id, getAddr(raft.point), protocol.Master)
			cores := make([]*core, 0, len(raft.cores)-1)
			cores = append(cores, c)
			for i := range voteList {
				if voteList[i].GetConn() != nil {
					wg.Add(1)
					go func(c *core) {
						protocol.SendMessage(c.GetConn(), protocol.Master, protocol.Endpoint, protocol.VICTORY, i64s(victor))
						log.Println("胜选信息已通知:", c.GetConn().RemoteAddr().String())
						mes, err := protocol.WaitMessage(c.GetConn())
						if err != nil || mes == nil {
							log.Println("胜选回信错误信息,mes:", mes)
							log.Println("胜选回信错误原因,err:", err)
						} else if mes.GetOperation() == protocol.ACK {
							log.Printf("节点 %s 已收到我胜选的信息", c.GetConn().RemoteAddr().String())
							go raft.listenCore(c.GetConn(), c.Id)
							cores = append(cores, c)
						}
						wg.Done()
					}(voteList[i])
				}
			}
			wg.Wait()
			raft.cores = cores
			go raft.clusterKeep()
		} else {
			log.Println("我败选了")
			//通知支持我的机器最终获胜者,为0则说明不满足半数原则,为防止脑裂直接中断
			for i := range voteList {
				if voteList[i].GetVote() == raft.id {
					wg.Add(1)
					go func(conn net.Conn) {
						protocol.SendMessage(conn, protocol.Master, protocol.Endpoint, protocol.VICTORY, i64s(victor))
						log.Println("败选信息已通知:", conn.RemoteAddr().String())
						mes, err := protocol.WaitMessage(conn)
						if err != nil || mes == nil {
							log.Println("败选回信错误信息,mes:", mes)
							log.Println("败选回信错误原因,err:", err)
						} else if mes.GetOperation() == protocol.ACK {
							log.Printf("节点 %s 已收到我败选的信息", conn.RemoteAddr().String())
						}
						wg.Done()
					}(voteList[i].GetConn())
				}
			}
			wg.Wait()
			masterAddr := raft.getMasterAddress(victor)
			if masterAddr != "" {
				raft.accessCluster(masterAddr, getAddr(raft.point))
			}
		}
	}
}
func (raft *Raft) voter(msg *protocol.Message, conn net.Conn) {
	if raft.isKeep() {
		log.Println("主节点仍存在")
		protocol.SendMessage(conn, protocol.Endpoint, protocol.Master, protocol.KEEP, "")
		return
	}
	log.Printf("%s 让我支持:%s ,我当前支持:%d\n", conn.RemoteAddr().String(), msg.GetContent(), raft.vote)
	if atomic.CompareAndSwapInt64(&raft.vote, 0, s64(msg.GetContent())) {
		protocol.SendMessage(conn, protocol.Endpoint, protocol.Master, protocol.ACK, i64s(raft.vote))
		mes, err := protocol.GetMessage(conn)
		if err != nil || mes == nil {
			log.Println("投票回信信息,mes:", mes)
			log.Println("投票回信错误,err:", err)
		} else if mes.GetOperation() == protocol.VICTORY {
			protocol.SendMessage(conn, protocol.Endpoint, protocol.Endpoint, protocol.ACK, "")
			log.Println("收到获胜节点:", mes.GetContent())
			if i64s(raft.vote) == mes.GetContent() {
				raft.master = conn
				go raft.listenMaster()
			} else {
				masterAddr := raft.getMasterAddress(s64(mes.GetContent()))
				if masterAddr != "" {
					raft.accessCluster(masterAddr, getAddr(raft.point))
				}
				conn.Close()
			}
		}
	} else {
		// 我已支持其他节点
		log.Println("我已支持其他节点:", raft.vote)
		protocol.SendMessage(conn, protocol.Endpoint, protocol.Master, protocol.ACK, i64s(raft.vote))
		conn.Close()
	}
}
func (raft *Raft) getMasterAddress(victor int64) string {
	masterAddr := ""
	for i := range raft.cores {
		if raft.cores[i].Id == victor {
			masterAddr = raft.cores[i].Addr
		}
	}
	return masterAddr
}
