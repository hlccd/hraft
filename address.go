package hraft

import (
	"fmt"
	"strings"
)

func getAddr(point int) string {
	return ":" + fmt.Sprintf("%d", point)
}

// 将 getAddr 转化为 ip 为 senderAddr 的ip ,从而获取到对方的实际链接地址
func addressTransition(senderAddr string, getAddr string) (address string) {
	tmpSender := strings.SplitN(senderAddr, ":", 2)
	tmpGet := strings.SplitN(getAddr, ":", 2)
	if len(tmpSender) != 2 || len(tmpGet) != 2 {
		return ""
	}
	if tmpGet[0] != "" {
		ss := strings.SplitN(tmpGet[0], ".", 4)
		if len(ss) != 4 {
			return ""
		} else {
			if ss[0] == "127" && (ss[1] != "0" || ss[2] != "0" || ss[3] != "0") {
				tmpGet[0] = tmpSender[0]
			}
		}
	} else {
		tmpGet[0] = tmpSender[0]
	}
	return tmpGet[0] + ":" + tmpGet[1]
}
