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
	//keySize, valueSize, ExtraSize, crc32 均为 uint32 类型，各占 4 字节
	//Type 和 Mark 占 2 + 2
	//4 + 4 + 4 + 4 + 2 + 2 = 20
	entryHeaderSize = 20
)

//Value的数据结构类型
const (
	String uint16 = iota
	List
	Hash
	Set
	ZSet
)

type (
	// Entry 数据entry定义
	Entry struct {
		Meta  *Meta
		Type  uint16 //数据类型
		Mark  uint16 //数据操作类型
		crc32 uint32 //校验和
	}

	// Meta meta 数据
	Meta struct {
		Key       []byte
		Value     []byte
		Extra     []byte //操作Entry所需的额外信息
		KeySize   uint32
		ValueSize uint32
		ExtraSize uint32
	}
)

// NewEntry new an entry
func NewEntry(key, value, extra []byte, t, mark uint16) *Entry {
	return &Entry{
		Meta: &Meta{
			Key:       key,
			Value:     value,
			Extra:     extra,
			KeySize:   uint32(len(key)),
			ValueSize: uint32(len(value)),
			ExtraSize: uint32(len(extra)),
		},
		Type: t,
		Mark: mark,
	}
}

// NewEntryNoExtra new an entry without extra info
func NewEntryNoExtra(key, value []byte, t, mark uint16) *Entry {
	return NewEntry(key, value, nil, t, mark)
}

// Size 返回entry的大小（包括header和key和value）
func (e *Entry) Size() uint32 {
	return entryHeaderSize + e.Meta.KeySize + e.Meta.ValueSize + e.Meta.ExtraSize
}

// Encode 对Entry进行编码，返回字节数组
func (e *Entry) Encode() ([]byte, error) {
	if e == nil || e.Meta.KeySize == 0 {
		return nil, ErrInvalidEntry
	}

	ks, vs := e.Meta.KeySize, e.Meta.ValueSize
	es := e.Meta.ExtraSize
	buf := make([]byte, e.Size())

	binary.BigEndian.PutUint32(buf[4:8], ks)   //  写入key的大小
	binary.BigEndian.PutUint32(buf[8:12], vs)  //  写入value的大小
	binary.BigEndian.PutUint32(buf[12:16], es) // 写入extra信息的大小
	binary.BigEndian.PutUint16(buf[16:18], e.Type)
	binary.BigEndian.PutUint16(buf[18:20], e.Mark)
	copy(buf[entryHeaderSize:entryHeaderSize+ks], e.Meta.Key)           //  写入key
	copy(buf[entryHeaderSize+ks:(entryHeaderSize+ks+vs)], e.Meta.Value) // 写入value

	if es > 0 { // 如果有extra info，就将其写入到buf中
		copy(buf[(entryHeaderSize+ks+vs):(entryHeaderSize+ks+vs+es)], e.Meta.Extra)
	}

	crc := crc32.ChecksumIEEE(e.Meta.Value)   // 计算校验和
	binary.BigEndian.PutUint32(buf[0:4], crc) // 第一部分 写入校验和 crc

	return buf, nil
}

// Decode 解码字节数组，返回Entry
func Decode(buf []byte) (*Entry, error) {
	ks := binary.BigEndian.Uint32(buf[4:8])  // 取出 key的大小
	vs := binary.BigEndian.Uint32(buf[8:12]) // 取出 value的大小
	es := binary.BigEndian.Uint32(buf[12:16])
	t := binary.BigEndian.Uint16(buf[16:18])
	mark := binary.BigEndian.Uint16(buf[18:20])
	crc := binary.BigEndian.Uint32(buf[0:4]) // 取出 校验和 crc

	return &Entry{
		Meta: &Meta{
			KeySize:   ks,
			ValueSize: vs,
			ExtraSize: es,
		},
		Type:  t,
		Mark:  mark,
		crc32: crc,
	}, nil
}
