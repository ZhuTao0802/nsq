package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

func Pub(conn net.Conn, topicName string, message string) {

	params := fmt.Sprintf("PUB %s\n", topicName)
	conn.Write([]byte(params))

	msgSize := len(message)
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(msgSize))
	conn.Write(bs)
	conn.Write([]byte(message))
}

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:4150")
	if err != nil {
		return
	}

	conn.Write([]byte("  V2"))
	Pub(conn, "topic-hello", "welcome")
	time.Sleep(5 * time.Minute)
	conn.Close()
}
