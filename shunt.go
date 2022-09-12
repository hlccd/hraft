package hraft

import (
	"github.com/hlccd/hraft/protocol"
	"log"
	"net"
)

func (raft *Raft) shunt(msg *protocol.Message, conn net.Conn) {
	if msg.GetSender() == protocol.Endpoint && msg.GetOperation() == protocol.JOIN {
		// 新节点加入该集群
		go raft.joinCluster(msg, conn)
	}
	if msg.GetSender() == protocol.Master {
		if msg.GetOperation() == protocol.AddCore {
			log.Println("新增节点")
			go raft.addCore(msg)
		}
		if msg.GetOperation() == protocol.DelCore {
			log.Println("删除节点")
			go raft.delCore(msg)
		}
		if msg.GetOperation() == protocol.KEEP {
			log.Println("保活")
			go raft.keep()
		}
		if msg.GetOperation() == protocol.UPDATE {
			log.Println("集群信息更新")
			go raft.update(msg)
		}
		if msg.GetOperation() == protocol.VOTE {
			go raft.voter(msg, conn)
		}
	}
	if msg.GetOperation() == protocol.MSG {
		if msg.GetSender() == protocol.Master {
			go raft.received(msg.GetContent())
		}
		if msg.GetSender() == protocol.Endpoint {
			go raft.broadcast(msg)
		}
	}
}
