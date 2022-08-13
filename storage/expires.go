package storage

import (
	"encoding/binary"
	"io"
	"log"
	"os"
)

//保存和加载过期字典

const expireHeadSize = 12

// Expires 过期字典定义
type Expires map[string]uint32

// ExpiresValue 过期值
type ExpiresValue struct {
	Key      []byte
	KeySize  uint32
	Deadline uint64
}

// SaveExpires 持久化过期字典信息
func (e *Expires) SaveExpires(path string) (err error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600) // 打开一个path下的写权限文件
	if err != nil {
		return err
	}
	defer file.Close()

	var offset int64 = 0
	for k, v := range *e { // 设置每个key相应的过期时间
		ev := &ExpiresValue{
			Key:      []byte(k),
			KeySize:  uint32(len([]byte(k))),
			Deadline: uint64(v),
		}

		buf := make([]byte, ev.KeySize+expireHeadSize)
		binary.BigEndian.PutUint32(buf[0:4], ev.KeySize) // 先写keySize 后写过期时间  最后放Key
		binary.BigEndian.PutUint64(buf[4:12], ev.Deadline)
		copy(buf[expireHeadSize:], ev.Key)

		_, err = file.WriteAt(buf, offset) // 在path下的文件中写入过期数据
		if err != nil {
			return
		}
		offset += int64(expireHeadSize + ev.KeySize) // 更新offset
	}
	return
}

// LoadExpires 从数据文件加载过期字典信息
func LoadExpires(path string) (expires Expires) {
	expires = make(Expires)
	file, err := os.OpenFile(path, os.O_RDONLY, 0600) // 只读权限打开path下的数据文件
	if err != nil {
		return
	}
	defer file.Close()

	var offset int64 = 0
	for {
		ev, err := readExpire(file, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println("load expire err : ", err)
			return
		}
		offset += int64(ev.KeySize + expireHeadSize)
		expires[string(ev.Key)] = uint32(ev.Deadline)
	}
	return
}

// 读取数据文件中一个key的过期时间
func readExpire(file *os.File, offset int64) (ev *ExpiresValue, err error) {
	buf := make([]byte, expireHeadSize)
	_, err = file.ReadAt(buf, offset) // 读一条过期时间记录
	if err != nil {
		return
	}

	ev = decodeExpire(buf)
	offset += int64(expireHeadSize)
	key := make([]byte, ev.KeySize)
	_, err = file.ReadAt(key, offset)
	if err != nil {
		return
	}
	ev.Key = key
	return
}

// 解码
func decodeExpire(buf []byte) *ExpiresValue {
	ev := &ExpiresValue{}
	ev.KeySize = binary.BigEndian.Uint32(buf[0:4])
	ev.Deadline = binary.BigEndian.Uint64(buf[4:12])
	return ev
}
