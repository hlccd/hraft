package hraft

import (
	"fmt"
	"strconv"
)

func s64(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return i
}
func i64s(i int64) string {
	return fmt.Sprintf("%d", i)
}
