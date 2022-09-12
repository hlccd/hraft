package hraft

import (
	"encoding/json"
	"github.com/hlccd/hraft/protocol"
	"log"
	"net"
	"time"
)

//  接入集群
func (raft *Raft) accessCluster(clusterAddr string, addr string) bool {
	conn, err := net.Dial("tcp", clusterAddr)
	log.Println("clusterAddr", clusterAddr)
	if err != nil {
		return false
	}
	protocol.SendMessage(conn, protocol.Endpoint, protocol.Master, protocol.JOIN, addr)
	mes, err := protocol.WaitMessage(conn)
	if err != nil {
		log.Println("接入集群失败,err:", err)
		return false
	}
	if mes.GetOperation() == protocol.ACK {
		//接入集群成功,将附带的节点列表的信息进行记载
		var cores []*core
		err := json.Unmarshal([]byte(mes.GetContent()), &cores)
		if err == nil {
			raft.cores = cores
			raft.master = conn
			// 主节点在允许该节点加入时候必然会上锁,使得该节点必然是最后一个加入的
			raft.id = cores[len(cores)-1].Id
			raft.vote = 0
			go raft.listenMaster()
		}
	} else if mes.GetOperation() == protocol.REDIRECT {
		//接入集群失败,附带信息为主节点地址,进行重新接入继可
		log.Println("重定向再次接入集群")
		return raft.accessCluster(addressTransition(conn.RemoteAddr().String(), mes.GetContent()), addr)
	}
	return true
}

func (raft *Raft) joinCluster(msg *protocol.Message, conn net.Conn) {
	log.Println("收到了一条节点加入集群的请求:", conn.RemoteAddr().String())
	if raft.IsMaster() {
		// 我是主节点,同意该节点接入
		raft.Lock()
		defer raft.Unlock()
		// 上锁添加该节点,保证该节点必然是最后添加进来的
		add := addressTransition(conn.RemoteAddr().String(), msg.GetContent())
		c := newCore(int64(time.Now().UnixNano()), add, protocol.Endpoint)
		c.SetConn(conn)
		cc, err := json.Marshal(c)
		raft.cores = append(raft.cores, c)
		v, err := json.Marshal(raft.cores)
		if err == nil {
			log.Println("同意该节点加入集群", c.Addr)
			//反馈该节点信息,同时将新节点的信息同步给其他所有节点
			protocol.SendMessage(conn, protocol.Master, protocol.Endpoint, protocol.ACK, string(v))
			for i := 0; i < len(raft.cores)-1; i++ {
				if raft.cores[i].Id == raft.id {
					continue
				}
				protocol.SendMessage(raft.cores[i].GetConn(), protocol.Master, protocol.Endpoint, protocol.AddCore, string(cc))
			}
			go raft.listenCore(conn, c.Id)
		}
	} else {
		// 我不是主节点,将主节点的地址发给该节点
		raft.RLock()
		defer raft.RUnlock()
		for i := range raft.cores {
			if raft.cores[i].Power == protocol.Master {
				protocol.SendMessage(conn, protocol.Endpoint, protocol.Endpoint, protocol.REDIRECT, raft.cores[i].Addr)
			}
		}
	}
}

// 监听主节点
func (raft *Raft) listenMaster() {
	log.Println("接入集群成功,正在监听master:", raft.master.RemoteAddr().String())
	raft.lastKeep = time.Now().UnixNano()
	has := true
	go func() {
		for has {
			time.Sleep(time.Minute)
			if has && !raft.isKeep() {
				//发起master选举
				has = false
				raft.voteMaster()
			}
		}
	}()

	for has {
		msg, err := protocol.GetMessage(raft.master)
		if err != nil {
			log.Println("主节点已断开", raft.master.RemoteAddr().String())
			raft.master.Close()
			has = false
			raft.voteMaster()
		}
		if err == nil && msg != nil {
			// 分流处理信息
			go raft.shunt(msg, raft.master)
		}
	}
}
func (raft *Raft) isKeep() bool {
	return time.Now().UnixNano()-raft.lastKeep < int64(time.Minute)
}

// 监听子节点
func (raft *Raft) listenCore(conn net.Conn, id int64) {
	log.Println("监听子节点:", conn.RemoteAddr().String())
	for {
		msg, err := protocol.GetMessage(conn)
		if err != nil {
			log.Println("子节点已断开", conn.RemoteAddr().String())
			conn.Close()
			raft.destroyCore(id)
			break
		}
		if err == nil && msg != nil {
			// 分流处理信息
			go raft.shunt(msg, raft.master)
		}
	}
}

// 主节点向其他节点通知将要销毁某一节点
func (raft *Raft) destroyCore(id int64) {
	log.Println("集群减少了节点,告知所有剩余节点")
	raft.Lock()
	cores := make([]*core, 0, len(raft.cores)-1)
	for i := range raft.cores {
		if raft.cores[i].Id != id {
			cores = append(cores, raft.cores[i])
		}
	}
	raft.cores = cores
	raft.Unlock()
	ids := i64s(id)
	for i := 0; i < len(cores); i++ {
		if raft.cores[i].Id == raft.id {
			continue
		}
		protocol.SendMessage(raft.cores[i].GetConn(), protocol.Master, protocol.Endpoint, protocol.AddCore, ids)
	}
}

// 集群新增了某节点
func (raft *Raft) addCore(msg *protocol.Message) {
	log.Println("集群新增了节点")
	var c *core
	err := json.Unmarshal([]byte(msg.GetContent()), &c)
	if err != nil {
		log.Println("解析新增的节点信息错误")
		return
	}
	raft.Lock()
	defer raft.Unlock()
	raft.cores = append(raft.cores, c)

}

// 集群减少了某节点
func (raft *Raft) delCore(msg *protocol.Message) {
	log.Println("集群减少了节点")
	var c *core
	err := json.Unmarshal([]byte(msg.GetContent()), &c)
	if err != nil {
		log.Println("解析新增的节点信息错误")
		return
	}
	id := s64(msg.GetContent())
	raft.Lock()
	defer raft.Unlock()
	cores := make([]*core, 0, len(raft.cores)-1)
	for i := range raft.cores {
		if raft.cores[i].Id != id {
			cores = append(cores, raft.cores[i])
		}
	}
	raft.cores = cores
}

// 集群保活
func (raft *Raft) clusterKeep() {
	for i := 0; i >= 0 && raft.IsMaster(); i++ {
		time.Sleep(10 * time.Second)
		raft.RLock()
		if raft.IsMaster() {
			for j := range raft.cores {
				protocol.SendMessage(raft.cores[j].GetConn(), protocol.Master, protocol.Endpoint, protocol.KEEP, "")
			}
			if i >= 5 {
				ss, err := json.Marshal(raft.cores)
				if err == nil {
					for j := range raft.cores {
						if raft.cores[j].Id == raft.id {
							continue
						}
						protocol.SendMessage(raft.cores[j].GetConn(), protocol.Master, protocol.Endpoint, protocol.UPDATE, string(ss))
					}
				}
				i = 0
			}
		}
		raft.RUnlock()
	}
}

func (raft *Raft) keep() {
	raft.Lock()
	defer raft.Unlock()
	raft.lastKeep = time.Now().UnixNano()
	log.Println("lastKeep:", raft.lastKeep)
}

func (raft *Raft) update(msg *protocol.Message) {
	raft.RLock()
	masterAddr := raft.master.RemoteAddr().String()
	raft.RUnlock()
	var cores []*core
	json.Unmarshal([]byte(msg.GetContent()), &cores)
	for i := range cores {
		cores[i].Addr = addressTransition(masterAddr, cores[i].Addr)
	}
	raft.Lock()
	raft.cores = cores
	raft.Unlock()
	log.Println("更新后的集群信息集:", raft.cores)
}

// 节点收到信息
func (raft *Raft) received(s string) {
	log.Println("收到了信息:", s)
	raft.read <- s
}

// 主节点向集群广播
func (raft *Raft) broadcast(msg *protocol.Message) {
	raft.RLock()
	for _, c := range raft.cores {
		if c.Power == protocol.Master {
			continue
		}
		protocol.SendMessage(c.GetConn(), protocol.Master, protocol.Endpoint, protocol.MSG, msg.GetContent())
	}
	raft.RUnlock()
	go raft.received(msg.GetContent())
}
