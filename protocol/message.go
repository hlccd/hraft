package protocol

import (
	"errors"
	"fmt"
)

const (
	headerLength = 5 //头部长度,单位 byte
)

type getMessageInfo interface {
	GetContent() string
	GetSender() uint8
	GetReceiver() uint8
	GetOperation() uint8
}

type Message struct {
	sender    uint8  //实际占4bit,发送者身份
	receiver  uint8  //实际占4bit,接收者身份
	operation uint8  //实际占8bit,操作
	ttl       uint8  //实际占4bit,后续使用
	window    uint16 //窗口大小,实际占12bit,后续使用
	checksum  uint8  //首部校验和
	content   string //携带信息
}

// CreateMessage 创建通信信息
func CreateMessage(sender, receiver, operation uint8, content string) (m *Message) {
	m = &Message{
		sender:    sender,
		receiver:  receiver,
		operation: operation,
		ttl:       0,
		window:    0,
		checksum:  uint8(0),
		content:   content,
	}
	m.checksum = ^countSum(m.toBytes()) + 1
	return m
}

// AnalysisMessage 解析通信信息
func AnalysisMessage(mes []byte) (m *Message, err error) {
	if len(mes) < headerLength {
		return m, errors.New("头部信息长度错误")
	}
	m = bytesToMessage(mes)
	if len(m.content) > 0 && mes[len(mes)-1] == Separator {
		m.content = m.content[:len(m.content)-1]
	}
	if m.checksum+countSum(mes) != 0 {
		return m, errors.New("校验错误")
	}
	return m, nil
}

// 头部信息求和
func countSum(bs []byte) (sum uint8) {
	sum = 0
	tmp := uint16(0)
	for i := 0; i < headerLength-1; i++ {
		tmp += uint16(bs[i])
		sum = uint8(tmp>>8) + uint8(tmp)
		tmp = uint16(sum)
	}
	return sum
}

// 将信息转化为 bytes 进行通信
func (m *Message) toBytes() (bs []byte) {
	if m == nil {
		return bs
	}
	bs = make([]byte, headerLength)
	bs[0] = m.sender*16 + m.receiver%16
	bs[1] = m.operation
	bs[2] = m.ttl*16 + byte((m.window>>8)%16)
	bs[3] = byte(m.window)
	bs[headerLength-1] = m.checksum
	return bs
}

// 将 bytes 转化为头部信息
func bytesToMessage(bs []byte) (m *Message) {
	return &Message{
		sender:    bs[0] / 16,
		receiver:  bs[0] % 16,
		operation: bs[1],
		ttl:       bs[2] / 16,
		window:    uint16(bs[2]%16)<<8 + uint16(bs[3]),
		checksum:  bs[4],
		content:   string(bs[headerLength:]),
	}
}

// 将通信信息转化为 string 格式进行传递
func (m *Message) String() string {
	if m == nil {
		return ""
	}
	s := fmt.Sprintf("%s%s%c", m.toBytes(), m.content, Separator)
	return s
}

// GetContent 获取附带信息
func (m *Message) GetContent() string {
	if m == nil {
		return ""
	}
	return m.content
}
func (m *Message) GetSender() uint8 {
	if m == nil {
		return ErrorPoint
	}
	return m.sender
}
func (m *Message) GetReceiver() uint8 {
	if m == nil {
		return ErrorPoint
	}
	return m.sender
}
func (m *Message) GetOperation() uint8 {
	if m == nil {
		return ErrorOperation
	}
	return m.operation
}
