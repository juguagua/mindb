package index

import (
	"encoding/binary"
	"io"
	"mindb/ds/skiplist"
	"os"
)

const (
	indexerHeaderSize = 4*4 + 8
)

//数据索引定义
type Indexer struct {
	Key       []byte
	Value     []byte
	FileId    uint32 //存储数据的文件id
	EntrySize uint32 //数据条目(Entry)的大小
	Offset    int64  //Entry数据的查询起始位置
	KeySize   uint32
	ValueSize uint32
}

// 返回数据索引（indexer）的大小
func (i *Indexer) Size() uint32 {
	return i.KeySize + i.ValueSize + indexerHeaderSize
}

//加载索引信息
func Build(t *skiplist.SkipList, path string) error {
	file, err := os.OpenFile(path, os.O_RDONLY, 0600) // 以只读方式打开path路径下的文件
	if err != nil {
		return err
	}

	defer file.Close()

	var offset int64 = 0
	for {

		buf := make([]byte, indexerHeaderSize)
		if _, err := file.ReadAt(buf, offset); err != nil { // 从offset（0）处开始读len(buf)的数据到buf中
			if err == io.EOF { // 读满了再读就退出循环
				break
			}
			return err
		}

		ks := binary.BigEndian.Uint32(buf[16:20]) // keySize 4个字节
		vs := binary.BigEndian.Uint32(buf[20:24]) // valueSize 4个字节
		idx := &Indexer{
			FileId:    binary.BigEndian.Uint32(buf[:4]),          // 文件id  4个字节
			EntrySize: binary.BigEndian.Uint32(buf[4:8]),         // entry size 4个字节
			Offset:    int64(binary.BigEndian.Uint64(buf[8:16])), // offset 8个字节
			KeySize:   ks,
			ValueSize: vs,
		}

		keyVal := make([]byte, ks+vs)
		if _, err = file.ReadAt(keyVal, indexerHeaderSize+offset); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		idx.Key, idx.Value = keyVal[:ks], keyVal[ks:ks+vs] // 将key和value值放入索引数据中
		t.Put(idx.Key, idx)                                // 存到索引结构中

		offset += int64(idx.Size()) // 更新offset
	}

	return nil
}

//保存索引信息
func Store(t *skiplist.SkipList, path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600) // 以只写权限打开path下的文件
	if err != nil {
		return err
	}

	defer file.Close()

	if t.Size > 0 {
		var offset int64 = 0
		handleFunc := func(e *skiplist.Element) bool {
			item := e.Value().(*Indexer)
			if item != nil {
				b := item.encode()
				if n, err := file.WriteAt(b, offset); err != nil { // 把编码后的indexer写入到file中
					return false
				} else {
					offset += int64(n) // 更新offset
				}
			}
			return true
		}

		t.Foreach(handleFunc) // 遍历跳表中的每个元素，执行写文件操作
	}

	if err := file.Sync(); err != nil { // 将文件刷到磁盘中
		return err
	}

	return nil
}

// 索引数据的二进制编码操作
func (i *Indexer) encode() []byte {
	buf := make([]byte, i.Size())

	ks, vs := len(i.Key), len(i.Value)
	binary.BigEndian.PutUint32(buf[0:4], i.FileId)
	binary.BigEndian.PutUint32(buf[4:8], i.EntrySize)
	binary.BigEndian.PutUint64(buf[8:16], uint64(i.Offset))
	binary.BigEndian.PutUint32(buf[16:20], i.KeySize)
	binary.BigEndian.PutUint32(buf[20:24], i.ValueSize)

	copy(buf[indexerHeaderSize:indexerHeaderSize+ks], i.Key)
	copy(buf[indexerHeaderSize+ks:indexerHeaderSize+ks+vs], i.Value)

	return buf
}
