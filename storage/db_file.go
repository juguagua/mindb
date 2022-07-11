package storage

import (
	"errors"
	"github.com/edsrzf/mmap-go"
)

const (
	//默认的创建文件权限, 0644表示用户具有读写权限，组用户和其他用户具有只读权限
	FilePerm = 0644

	//默认数据文件名称格式化
	DBFileFormatName = "%09d.data"
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
	id     uint8
	path   string
	file   *os.File
	mmap   mmap.MMap
	Offset int64
	method FileRWMethod
}
