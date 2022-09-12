package hraft

import (
	"net"
	"sync"
)

type core struct {
	Id    int64    `json:"id"` //该节点的id
	vote  int64    //该节点投票支持的节点
	Addr  string   `json:"addr"`  //该节点对外开放的地址
	Power uint8    `json:"power"` //该节点的权限
	conn  net.Conn //对该节点的连接
	sync.Mutex
}

func newCore(id int64, addr string, power uint8) *core {
	return &core{
		Id:    id,
		Addr:  addr,
		Power: power,
		conn:  nil,
		vote:  0,
	}
}
func (c *core) SetConn(conn net.Conn) {
	if c != nil {
		c.conn = conn
	}
}

func (c *core) GetConn() net.Conn {
	if c == nil {
		return nil
	}
	return c.conn
}

func (c *core) SetVote(vote int64) bool {
	if c == nil {
		return false
	}
	c.Lock()
	defer c.Unlock()
	if c.vote != 0 {
		return false
	}
	c.vote = vote
	return true
}

func (c *core) GetVote() int64 {
	if c == nil {
		return 0
	}
	return c.vote
}
