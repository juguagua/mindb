package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/peterh/liner"
	"log"
	"net"
	"os"
	"strings"
)

// all supported commands
var commandList = [][]string{
	{"SET", "key value", "STRING"},
	{"GET", "key", "STRING"},
	{"SETNX", "key value", "STRING"},
	{"GETSET", "key value", "STRING"},
	{"APPEND", "key value", "STRING"},
	{"STRLEN", "key", "STRING"},
	{"STREXISTS", "key", "STRING"},
	{"STRREM", "key", "STRING"},
	{"PREFIXSCAN", "prefix limit offset", "STRING"},
	{"RANGESCAN", "start end", "STRING"},
	{"EXPIRE", "key seconds", "STRING"},
	{"PERSIST", "key", "STRING"},
	{"TTL", "key", "STRING"},

	{"LPUSH", "key value [value...]", "LIST"},
	{"RPUSH", "key value [value...]", "LIST"},
	{"LPOP", "key", "LIST"},
	{"RPOP", "key", "LIST"},
	{"LINDEX", "key index", "LIST"},
	{"LREM", "key value count", "LIST"},
	{"LINSERT", "key BEFORE|AFTER pivot element", "LIST"},
	{"LSET", "key index value", "LIST"},
	{"LTRIM", "key start end", "LIST"},
	{"LRANGE", "key start end", "LIST"},
	{"LLEN", "key", "LIST"},

	{"HSET", "key field value", "HASH"},
	{"HSETNX", "key field value", "HASH"},
	{"HGET", "key field", "HASH"},
	{"HGETALL", "key", "HASH"},
	{"HDEL", "key field [field...]", "HASH"},
	{"HEXISTS", "key field", "HASH"},
	{"HLEN", "key", "HASH"},
	{"HKEYS", "key", "HASH"},
	{"HVALUES", "key", "HASH"},

	{"SADD", "key members [members...]", "SET"},
	{"SPOP", "key count", "SET"},
	{"SISMEMBER", "key member", "SET"},
	{"SRANDMEMBER", "key count", "SET"},
	{"SREM", "key members [members...]", "SET"},
	{"SMOVE", "src dst member", "SET"},
	{"SCARD", "key", "key", "SET"},
	{"SMEMBERS", "key", "SET"},
	{"SUNION", "key [key...]", "SET"},
	{"SDIFF", "key [key...]", "SET"},

	{"ZADD", "key score member", "ZSET"},
	{"ZSCORE", "key member", "ZSET"},
	{"ZCARD", "key", "ZSET"},
	{"ZRANK", "key member", "ZSET"},
	{"ZREVRANK", "key member", "ZSET"},
	{"ZINCRBY", "key increment member", "ZSET"},
	{"ZRANGE", "key start stop", "ZSET"},
	{"ZREVRANGE", "key start stop", "ZSET"},
	{"ZREM", "key member", "ZSET"},
	{"ZGETBYRANK", "key rank", "ZSET"},
	{"ZREVGETBYRANK", "key rank", "ZSET"},
	{"ZSCORERANGE", "key min max", "ZSET"},
	{"ZREVSCORERANGE", "key max min", "ZSET"},
}

var host = flag.String("h", "127.0.0.1", "the mindb server host, default 127.0.0.1")
var port = flag.Int("p", 5200, "the mindb server port, default 5200")

const cmdHistoryPath = "/tmp/mindb-cli"

func main() {
	flag.Parse() // 解析配置

	addr := fmt.Sprintf("%s:%d", *host, *port)
	conn, err := net.Dial("tcp", addr) // 与服务器建立连接
	if err != nil {
		log.Println("tcp dial err: ", err)
		return
	}

	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true) // 设置当ctrl + c的时候退出
	line.SetCompleter(func(li string) (res []string) {
		for _, c := range commandList {
			if strings.HasPrefix(c[0], strings.ToUpper(li)) {
				res = append(res, strings.ToLower(c[0]))
			}
		}
		return
	})

	// open and save cmd history
	if f, err := os.Open(cmdHistoryPath); err == nil {
		line.ReadHistory(f)
		f.Close()
	}
	defer func() {
		if f, err := os.Create(cmdHistoryPath); err != nil {
			fmt.Printf("writing cmd history err: %v\n", err)
		} else {
			line.WriteHistory(f)
			f.Close()
		}
	}()

	commandSet := map[string]bool{}
	for _, cmd := range commandList {
		commandSet[strings.ToLower(cmd[0])] = true
	}

	prompt := addr + ">"
	for {
		cmd, err := line.Prompt(prompt)
		if err != nil {
			fmt.Println(err)
			break
		}

		cmd = strings.TrimSpace(cmd)
		if len(cmd) == 0 {
			continue
		}
		lowerCmd := strings.ToLower(cmd)
		c := strings.Split(cmd, " ")

		if lowerCmd == "help" {
			printCmdHelp()
		} else if lowerCmd == "quit" {
			break
		} else if strings.ToLower(c[0]) == "help" && len(c) == 2 {
			helpCmd := strings.ToLower(c[1])
			if !commandSet[helpCmd] {
				fmt.Println("command not found")
				continue
			}

			for _, command := range commandList {
				if strings.ToLower(command[0]) == helpCmd {
					fmt.Println()
					fmt.Println(" --usage: " + helpCmd + " " + command[1])
					fmt.Println(" --group: " + command[2] + "\n")
				}
			}
		} else {
			line.AppendHistory(cmd)

			lowerC := strings.ToLower(strings.TrimSpace(c[0]))
			if !commandSet[lowerC] && lowerC != "quit" {
				continue
			}

			wInfo := wrapCmdInfo(cmd)   // 获取到命令
			_, err := conn.Write(wInfo) // 发送给服务端
			if err != nil {
				fmt.Println(err)
			}

			reply := readReply(conn) // 读取响应
			fmt.Println(reply)
		}
	}
}

func printCmdHelp() {
	help := `
 Thanks for using MinDB
 mindb-cli
 To get help about command:
 	Type: "help <command>" for help on command
 To quit:
	<ctrl+c> or <quit>`
	fmt.Println(help)
}

func wrapCmdInfo(cmd string) []byte {
	b := make([]byte, len(cmd)+4)
	binary.BigEndian.PutUint32(b[:4], uint32(len(cmd)))
	copy(b[4:], cmd)
	return b
}

func readReply(conn net.Conn) (res string) {
	reader := bufio.NewReader(conn)

	b := make([]byte, 4)
	_, err := reader.Read(b)
	if err != nil {
		return
	}
	size := binary.BigEndian.Uint32(b)
	if size > 0 {
		data := make([]byte, size)
		reader.Read(data)
		res = string(data)
	}
	return
}
