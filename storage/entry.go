package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var (
	ErrInvalidEntry = errors.New("storage/entry: invalid entry")
	ErrInvalidCrc   = errors.New("storage/entry: invalid crc")
)

const (
	//keySize, valueSize, crc32 均为 uint32 类型，各占 4 字节
	//4 + 4 + 4 = 12
	entryHeaderSize = 12
)

type Entry struct {
	Key       []byte
	Value     []byte
	keySize   uint32
	valueSize uint32
	crc32     uint32
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:       key,
		Value:     value,
		keySize:   uint32(len(key)),
		valueSize: uint32(len(value)),
	}
}

// 返回entry的大小（包括header和key和value）
func (e *Entry) Size() uint32 {
	return entryHeaderSize + e.keySize + e.valueSize
}

//对Entry进行编码，返回字节数组
func (e *Entry) Encode() ([]byte, error) {
	if e == nil || e.keySize == 0 {
		return nil, ErrInvalidEntry
	}

	ks, vs := e.keySize, e.valueSize
	buf := make([]byte, e.Size())

	binary.BigEndian.PutUint32(buf[4:8], ks)                       // 第二部分 写入key的大小
	binary.BigEndian.PutUint32(buf[8:12], vs)                      // 第三部分 写入value的大小
	copy(buf[entryHeaderSize:entryHeaderSize+ks], e.Key)           // 第四部分 写入key
	copy(buf[entryHeaderSize+ks:(entryHeaderSize+ks+vs)], e.Value) // 第五部分 写入value

	crc := crc32.ChecksumIEEE(e.Value)        // 计算校验和
	binary.BigEndian.PutUint32(buf[0:4], crc) // 第一部分 写入校验和 crc

	return buf, nil
}

//解码字节数组，返回Entry
func Decode(buf []byte) (*Entry, error) {
	ks := binary.BigEndian.Uint32(buf[4:8])                        // 取出第二部分 key的大小
	vs := binary.BigEndian.Uint32(buf[8:12])                       // 取出第三部分 value的大小
	key := buf[entryHeaderSize : entryHeaderSize+ks]               // 取出第四部分 key
	value := buf[entryHeaderSize+ks : (entryHeaderSize + ks + vs)] // 取出第五部分 vlaue
	crc := binary.BigEndian.Uint32(buf[0:4])                       // 取出第一部分 校验和 crc

	checkCrc := crc32.ChecksumIEEE(value)
	if checkCrc != crc { // 检查校验和是否正确
		return nil, ErrInvalidCrc
	}

	return &Entry{
		keySize:   ks,
		valueSize: vs,
		Key:       key,
		Value:     value,
		crc32:     crc,
	}, nil
}
