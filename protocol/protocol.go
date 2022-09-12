package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"time"
)

const (
	Separator = '\n' //末尾分隔符
)

// 权限
const (
	ErrorPoint = uint8(0) //错误节点
	Master     = uint8(1) //主节点
	Endpoint   = uint8(2) //边缘节点
)

// 操作类型
const (
	ErrorOperation = uint8(0)  //错误操作
	SYN            = uint8(1)  //
	ACK            = uint8(2)  //成功
	PSH            = uint8(3)  //推送消息
	GET            = uint8(4)  //获取信息
	NIL            = uint8(5)  //空
	FIN            = uint8(6)  //完成
	JOIN           = uint8(7)  //加入集群
	REDIRECT       = uint8(8)  //切换主节点
	KEEP           = uint8(9)  //保活
	AddCore        = uint8(11) //新增节点
	DelCore        = uint8(12) //删除节点
	MSG            = uint8(13) //消息流转
	VOTE           = uint8(15) //投票
	VICTORY        = uint8(16) //胜选
	VICTOR         = uint8(17) //发送胜利者id
	UPDATE         = uint8(18) //更新
)

// SendMessage 发送消息
func SendMessage(conn net.Conn, sender, receiver, operation uint8, msg string) {
	if conn == nil {
		return
	}
	mes := CreateMessage(sender, receiver, operation, msg)
	fmt.Fprintf(conn, mes.String())
}

// WaitMessage 等待信息并将其进行解析
func WaitMessage(conn net.Conn) (msg *Message, err error) {
	if conn == nil {
		return nil, errors.New("连接不存在")
	}
	ch := make(chan bool)
	msg = nil
	go func() {
		// 获取信息
		m, err := GetMessage(conn)
		if err != nil {
			ch <- false
		}
		msg = m
		ch <- true
	}()
	select {
	// 等待1s
	case <-time.After(time.Second):
		return nil, err
	case p := <-ch:
		if p {
			return msg, nil
		}
		return nil, err
	}
}

func GetMessage(conn net.Conn) (msg *Message, err error) {
	data, err := bufio.NewReader(conn).ReadString(Separator)
	if err != nil {
		log.Println("err:", err)
		conn.Close()
		return nil, err
	}
	// 解析信息
	msg, err = AnalysisMessage([]byte(data))
	if err != nil {
		log.Println("err:", err)
		return nil, err
	}
	return msg, nil
}
