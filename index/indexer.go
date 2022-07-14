package index

import (
	"mindb/storage"
)

// Indexer 数据索引定义
type Indexer struct {
	Meta      *storage.Meta //元数据信息
	FileId    uint32        //存储数据的文件id
	EntrySize uint32        //数据条目(Entry)的大小
	Offset    int64         //Entry数据的查询起始位置

}

//// 返回数据索引（indexer）的大小
//func (i *Indexer) Size() uint32 {
//	return i.Meta.KeySize + i.Meta.ValueSize + i.Meta.ExtraSize + indexerHeaderSize
//}
//
////加载索引信息
//func Build(t *SkipList, path string) error {
//	file, err := os.OpenFile(path, os.O_RDONLY, 0600) // 以只读方式打开path路径下的文件
//	if err != nil {
//		return err
//	}
//
//	defer file.Close()
//
//	var offset int64 = 0
//	for {
//
//		buf := make([]byte, indexerHeaderSize)
//		if _, err := file.ReadAt(buf, offset); err != nil { // 从offset（0）处开始读len(buf)的数据到buf中
//			if err == io.EOF { // 读满了再读就退出循环
//				break
//			}
//			return err
//		}
//
//		ks := binary.BigEndian.Uint32(buf[16:20]) // keySize 4个字节
//		vs := binary.BigEndian.Uint32(buf[20:24]) // valueSize 4个字节
//		es := binary.BigEndian.Uint32(buf[24:28])  // extraSize 4个字节
//		idx := &Indexer{
//			FileId:    binary.BigEndian.Uint32(buf[:4]),          // 文件id  4个字节
//			EntrySize: binary.BigEndian.Uint32(buf[4:8]),         // entry size 4个字节
//			Offset:    int64(binary.BigEndian.Uint64(buf[8:16])), // offset 8个字节
//			Meta: &storage.Meta{
//				KeySize:   ks,
//				ValueSize: vs,
//				ExtraSize: es,
//			},
//		}
//
//		val := make([]byte, ks+vs+es)
//		if _, err = file.ReadAt(val, indexerHeaderSize+offset); err != nil {
//			if err == io.EOF {
//				break
//			}
//			return err
//		}
//
//		idx.Meta.Key, idx.Meta.Value = val[:ks], val[ks:ks+vs] // 将key和value值放入索引数据中
//		if es > 0 {
//			idx.Meta.Extra = val[ks+vs : ks+vs+es]
//		}
//		t.Put(idx.Meta.Key, idx)                                // 存到索引结构中
//
//		offset += int64(idx.Size()) // 更新offset
//	}
//
//	return nil
//}
//
////保存索引信息
//func Store(t *SkipList, path string) error {
//	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600) // 以只写权限打开path下的文件
//	if err != nil {
//		return err
//	}
//
//	defer file.Close()
//
//	if t.Len > 0 {
//		var offset int64 = 0
//		handleFunc := func(e *Element) bool {
//			item := e.Value().(*Indexer)
//			if item != nil {
//				b := item.encode()
//				if n, err := file.WriteAt(b, offset); err != nil { // 把编码后的indexer写入到file中
//					return false
//				} else {
//					offset += int64(n) // 更新offset
//				}
//			}
//			return true
//		}
//
//		t.Foreach(handleFunc) // 遍历跳表中的每个元素，执行写文件操作
//	}
//
//	if err := file.Sync(); err != nil { // 将文件刷到磁盘中
//		return err
//	}
//
//	return nil
//}
//
//// 索引数据的二进制编码操作
//func (i *Indexer) encode() []byte {
//	buf := make([]byte, i.Size())
//
//	ks, vs, es := len(i.Meta.Key), len(i.Meta.Value), len(i.Meta.Extra)
//	binary.BigEndian.PutUint32(buf[0:4], i.FileId)
//	binary.BigEndian.PutUint32(buf[4:8], i.EntrySize)
//	binary.BigEndian.PutUint64(buf[8:16], uint64(i.Offset))
//	binary.BigEndian.PutUint32(buf[16:20], i.Meta.KeySize)
//	binary.BigEndian.PutUint32(buf[20:24], i.Meta.ValueSize)
//	binary.BigEndian.PutUint32(buf[24:28], i.Meta.ExtraSize)
//
//	copy(buf[indexerHeaderSize:indexerHeaderSize+ks], i.Meta.Key)
//	copy(buf[indexerHeaderSize+ks:indexerHeaderSize+ks+vs], i.Meta.Value)
//	if es > 0 {
//		copy(buf[indexerHeaderSize+ks+vs:indexerHeaderSize+ks+vs+es], i.Meta.Extra)
//	}
//
//	return buf
//}
