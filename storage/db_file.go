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
	// FilePerm 默认的创建文件权限, 0644表示用户具有读写权限，组用户和其他用户具有只读权限
	FilePerm = 0644

	// PathSeparator the default path separator
	PathSeparator = string(os.PathSeparator)
)

var (
	ErrEmptyEntry = errors.New("storage/db_file: entry or the Key of entry is empty")
)

var (
	// DBFileFormatNames 默认数据文件名称格式化
	DBFileFormatNames = map[uint16]string{
		0: "%09d.data.str",
		1: "%09d.data.list",
		2: "%09d.data.hash",
		3: "%09d.data.set",
		4: "%09d.data.zset",
	}

	// DBFileSuffixName represent the suffix names of the db files.
	DBFileSuffixName = []string{"str", "list", "hash", "set", "zset"}
)

// FileRWMethod 数据文件数据读写的方式
type FileRWMethod uint8

const (

	// FileIO 表示文件数据读写使用系统标准IO
	FileIO FileRWMethod = iota

	// MMap 表示文件数据读写使用Mmap
	// MMap 指的是将文件或其他设备映射至内存，具体可参考Wikipedia上的解释 https://en.wikipedia.org/wiki/Mmap
	MMap
)

// DBFile db数据文件定义
type DBFile struct {
	Id     uint32
	path   string
	File   *os.File
	mmap   mmap.MMap
	Offset int64
	method FileRWMethod
}

// NewDBFile 新建一个数据读写文件，如果是MMap，则需要Truncate文件并进行加载
func NewDBFile(path string, fileId uint32, method FileRWMethod, blockSize int64, eType uint16) (*DBFile, error) {
	filePath := path + PathSeparator + fmt.Sprintf(DBFileFormatNames[eType], fileId) // 要指定文件的数据类型

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
		m, err := mmap.Map(file, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		df.mmap = m
	}
	return df, nil
}

// Read 从数据文件中读数据 offset是读的起始位置
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

// 从数据文件中读数据 offset是读的起始位置，n表示读取多少字节
func (df *DBFile) readBuf(offset int64, n int64) ([]byte, error) {
	buf := make([]byte, n)

	if df.method == FileIO {
		_, err := df.File.ReadAt(buf, offset) // 从offset处开始读取buf大小的数据到buf切片中
		if err != nil {
			return nil, err
		}
	}

	if df.method == MMap && offset <= int64(len(df.mmap)) {
		copy(buf, df.mmap[offset:])
	}

	return buf, nil
}

// Write 从文件的offset处开始写数据
func (df *DBFile) Write(e *Entry) error {
	if e == nil || e.Meta.KeySize == 0 {
		return ErrEmptyEntry
	}

	method := df.method
	writeOff := df.Offset
	encVal, err := e.Encode()
	if err != nil {
		return err
	}

	if method == FileIO {
		if _, err := df.File.WriteAt(encVal, writeOff); err != nil {
			return err
		}
	}
	if method == MMap {
		copy(df.mmap[writeOff:], encVal)
	}

	df.Offset += int64(e.Size())
	return nil
}

// Close 读写后进行关闭操作
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

// Sync 数据持久化
func (df *DBFile) Sync() (err error) {
	if df.File != nil {
		err = df.File.Sync()
	}

	if df.mmap != nil {
		err = df.mmap.Flush()
	}
	return
}

// Build 加载数据文件
func Build(path string, method FileRWMethod, blockSize int64) (map[uint16]map[uint32]*DBFile, map[uint16]uint32, error) {
	dir, err := ioutil.ReadDir(path) // 读取该目录下的文件和目录
	if err != nil {
		return nil, nil, err
	}

	fileIdsMap := make(map[uint16][]int) // map的key为文件类型，val为文件id
	for _, d := range dir {
		if strings.Contains(d.Name(), ".data") { // 如果包含.data即是数据文件
			splitNames := strings.Split(d.Name(), ".")
			id, _ := strconv.Atoi(splitNames[0])
			switch splitNames[2] { // 根据文件类型加入到相应的文件id切片中
			case DBFileSuffixName[0]:
				fileIdsMap[0] = append(fileIdsMap[0], id)
			case DBFileSuffixName[1]:
				fileIdsMap[1] = append(fileIdsMap[1], id)
			case DBFileSuffixName[2]:
				fileIdsMap[2] = append(fileIdsMap[2], id)
			case DBFileSuffixName[3]:
				fileIdsMap[3] = append(fileIdsMap[3], id)
			case DBFileSuffixName[4]:
				fileIdsMap[4] = append(fileIdsMap[4], id)
			}
		}
	}

	// 加载所有的数据文件
	activeFileIds := make(map[uint16]uint32)         // 每个文件类型都有一个activeFile，所以用map来存储，存储每个类型的active file id
	archFiles := make(map[uint16]map[uint32]*DBFile) // 存储每个类型的文件id和其对应的数据文件
	var dataType uint16 = 0
	for ; dataType < 5; dataType++ { // 遍历每种类型的数据文件
		fileIDs := fileIdsMap[dataType]   // 取出相应类型的文件id切片
		sort.Ints(fileIDs)                // 排序
		files := make(map[uint32]*DBFile) // 文件id和数据文件的映射
		var activeFileId uint32 = 0

		if len(fileIDs) > 0 {
			activeFileId = uint32(fileIDs[len(fileIDs)-1]) // 最大的那个文件id为active file id

			for i := 0; i < len(fileIDs)-1; i++ {
				id := fileIDs[i]

				file, err := NewDBFile(path, uint32(id), method, blockSize, dataType)
				if err != nil {
					return nil, nil, err
				}
				files[uint32(id)] = file // 将arch file和其id对应起来
			}
		}
		archFiles[dataType] = files // 加入到相应的map映射中
		activeFileIds[dataType] = activeFileId
	}
	return archFiles, activeFileIds, nil
}
