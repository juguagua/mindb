package storage

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// DBMeta 保存数据库的一些额外信息
type DBMeta struct {
	ActiveWriteOff map[uint16]int64 `json:"active_write_off"` //当前数据文件的写偏移（分类型）
}

// LoadMeta 加载数据库信息
func LoadMeta(path string) (m *DBMeta, err error) {
	m = &DBMeta{ActiveWriteOff: make(map[uint16]int64)}

	file, err := os.OpenFile(path, os.O_RDONLY, 0600) // 只读权限打开path路径下的文件
	if err != nil {
		return
	}

	defer file.Close()

	b, err := ioutil.ReadAll(file) // 读取文件
	if err != nil {
		return
	}

	err = json.Unmarshal(b, m) // 解析json编码的数据到DBMeta中
	if err != nil {
		return
	}
	return
}

// Store 将数据库信息存储
func (m *DBMeta) Store(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	defer file.Close()

	b, err := json.Marshal(m) // 对DBMeta进行json编码
	if err != nil {
		return err
	}

	_, err = file.Write(b) // 写入到文件中
	return err
}
