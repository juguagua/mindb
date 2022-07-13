package storage

import (
	"errors"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"hash/crc32"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	//默认的创建文件权限, 0644表示用户具有读写权限，组用户和其他用户具有只读权限
	FilePerm = 0644

	//默认数据文件名称格式化
	DBFileFormatName = "%09d.data"

	PathSeparator = string(os.PathSeparator)
)

var (
	ErrEmptyEntry = errors.New("storage/db_file: entry or the Key of entry is empty")
)

//文件数据读写的方式
type FileRWMethod uint8

const (

	//FileIO 表示文件数据读写使用系统标准IO
	FileIO FileRWMethod = iota

	//MMap 表示文件数据读写使用Mmap
	//MMap 指的是将文件或其他设备映射至内存，具体可参考Wikipedia上的解释 https://en.wikipedia.org/wiki/Mmap
	MMap
)

type DBFile struct {
	Id     uint32
	path   string
	File   *os.File
	mmap   mmap.MMap
	Offset int64
	method FileRWMethod
}

//新建一个数据读写文件，如果是MMap，则需要Truncate文件并进行加载
func NewDBFile(path string, fileId uint32, method FileRWMethod, blockSize int64) (*DBFile, error) {
	filePath := path + PathSeparator + fmt.Sprintf(DBFileFormatName, fileId)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	df := &DBFile{Id: fileId, path: path, Offset: 0, method: method}

	if method == FileIO {
		df.File = file
	} else {
		if err = file.Truncate(blockSize); err != nil {
			return nil, err
		}
		if m, err := mmap.Map(file, os.O_RDWR, 0); err != nil {
			return nil, err
		} else {
			df.mmap = m
		}
	}
	return df, nil
}

//从数据文件中读数据 offset是读的起始位置
func (df *DBFile) Read(offset int64) (e *Entry, err error) {

	var buf []byte
	if buf, err = df.readBuf(offset, int64(entryHeaderSize)); err != nil { // 读取entry header信息到buf中
		return
	}

	if e, err = Decode(buf); err != nil { // 对buf进行解码得到entry
		return
	}

	offset += entryHeaderSize // 更新offset
	if e.Meta.KeySize > 0 {   // 如果解码出的entry中有key，就对其key进行赋值
		var key []byte
		if key, err = df.readBuf(offset, int64(e.Meta.KeySize)); err != nil {
			return
		}
		e.Meta.Key = key
	}

	offset += int64(e.Meta.KeySize) // 更新offset
	if e.Meta.ValueSize > 0 {       // 如果解码出的entry中有value，就对其key进行赋值
		var val []byte
		if val, err = df.readBuf(offset, int64(e.Meta.ValueSize)); err != nil {
			return
		}
		e.Meta.Value = val
	}

	offset += int64(e.Meta.ValueSize) // 更新offset
	if e.Meta.ExtraSize > 0 {         // 如果解码出的entry中有extra，就对其key进行赋值
		var val []byte
		if val, err = df.readBuf(offset, int64(e.Meta.ExtraSize)); err != nil {
			return
		}
		e.Meta.Extra = val
	}

	checkCrc := crc32.ChecksumIEEE(e.Meta.Value) // 计算校验和进行检验
	if checkCrc != e.crc32 {
		return nil, ErrInvalidCrc
	}

	return
}

//从数据文件中读数据 offset是读的起始位置，n表示读取多少字节
func (df *DBFile) readBuf(offset int64, n int64) ([]byte, error) {
	buf := make([]byte, n)

	if df.method == FileIO {
		_, err := df.File.ReadAt(buf, offset) // 从offset处开始读取buf大小的数据到buf切片中
		if err != nil {
			return nil, err
		}
	}

	if df.method == MMap {
		copy(buf, df.mmap[offset:])
	}

	return buf, nil
}

//从文件的offset处开始写数据
func (df *DBFile) Write(e *Entry) error {
	if e == nil || e.Meta.KeySize == 0 {
		return ErrEmptyEntry
	}

	method := df.method
	writeOff := df.Offset
	if encVal, err := e.Encode(); err != nil {
		return err
	} else {
		if method == FileIO {
			if _, err := df.File.WriteAt(encVal, writeOff); err != nil {
				return err
			}
		}

		if method == MMap {
			copy(df.mmap[writeOff:], encVal)
		}
	}

	df.Offset += int64(e.Size())
	return nil
}

//读写后进行关闭操作
func (df *DBFile) Close(sync bool) (err error) { //sync 关闭前是否持久化数据
	if sync {
		err = df.Sync()
	}

	if df.File != nil {
		err = df.File.Close()
	}
	if df.mmap != nil {
		err = df.mmap.Unmap()
	}
	return
}

//数据持久化
func (df *DBFile) Sync() (err error) {
	if df.File != nil {
		err = df.File.Sync()
	}

	if df.mmap != nil {
		err = df.mmap.Flush()
	}
	return
}

//加载数据文件
func Build(path string, method FileRWMethod, blockSize int64) (map[uint32]*DBFile, uint32, error) {
	dir, err := ioutil.ReadDir(path) // 读取该目录下的文件和目录
	if err != nil {
		return nil, 0, err
	}

	var fileIds []int
	for _, d := range dir {
		if strings.HasSuffix(d.Name(), "data") { // 如果是data为后缀即是数据文件
			splitNames := strings.Split(d.Name(), ".")
			id, _ := strconv.Atoi(splitNames[0])
			fileIds = append(fileIds, id) // 将数据文件的id加入到fileIds切片中
		}
	}

	sort.Ints(fileIds) // 对收集到的文件id进行排序
	var activeFileId uint32 = 0
	archFiles := make(map[uint32]*DBFile)
	if len(fileIds) > 0 {
		activeFileId = uint32(fileIds[len(fileIds)-1]) // active file一定是id最大（最后被写入）的文件

		for i := 0; i < len(fileIds)-1; i++ {
			id := fileIds[i]

			file, err := NewDBFile(path, uint32(id), method, blockSize) // 将文件封装起来
			if err != nil {
				return nil, activeFileId, err
			}

			archFiles[uint32(id)] = file // 将数据文件和它的id一一对应写到map中
		}
	}

	return archFiles, activeFileId, nil
}
