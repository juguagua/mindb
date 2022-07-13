package mindb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"mindb/ds/list"
	"mindb/index"
	"mindb/storage"
	"mindb/utils"
	"os"
	"sync"
)

var (
	ErrEmptyKey = errors.New("mindb: the key is empty")

	ErrKeyTooLarge = errors.New("mindb: key exceeded the max length")

	ErrValueTooLarge = errors.New("mindb: value exceeded the max length")

	ErrKeyNotExist = errors.New("mindb: key not exist")

	ErrNilIndexer = errors.New("mindb: indexer is nil")

	ErrCfgNotExist = errors.New("mindb: the config file not exist")

	ErrReclaimUnreached = errors.New("mindb: unused space not reach the threshold")

	ErrExtraContainsSeparator = errors.New("rosedb: extra contains separator \\0")
)

const (
	//保存配置的文件名称
	configSaveFile = string(os.PathSeparator) + "db.cfg"

	//保存索引状态的文件名称
	indexSaveFile = string(os.PathSeparator) + "db.idx"

	//保存数据库相关信息的文件名称
	dbMetaSaveFile = string(os.PathSeparator) + "db.meta"

	//回收磁盘空间时的临时目录
	reclaimPath = string(os.PathSeparator) + "mindb_reclaim"

	//额外信息的分隔符，用于存储一些额外的信息（因此一些操作的value中不能包含此分隔符）
	ExtraSeparator = "\\0"
)

type (
	MinDB struct {
		activeFile   *storage.DBFile
		archFiles    ArchivedFiles
		idxList      *index.SkipList
		listIndex    *list.List
		config       Config
		activeFileId uint32
		mu           sync.RWMutex
		meta         *storage.DBMeta
	}

	//已封存的文件map索引
	ArchivedFiles map[uint32]*storage.DBFile
)

//打开一个数据库实例
func Open(config Config) (*MinDB, error) {

	//如果目录不存在则创建
	if !utils.Exist(config.DirPath) {
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//如果存在索引文件，则加载索引状态
	skipList := index.NewSkipList()
	if utils.Exist(config.DirPath + indexSaveFile) {
		err := index.Build(skipList, config.DirPath+indexSaveFile) // 加载索引文件的信息到索引中
		if err != nil {
			return nil, err
		}
	}

	//加载数据文件
	archFiles, activeFileId, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}
	activeFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	//加载数据库额外信息（meta）
	meta := storage.LoadMeta(config.DirPath + dbMetaSaveFile)

	activeFile.Offset = meta.ActiveWriteOff // 更新当前活跃文件的写偏移

	db := &MinDB{
		activeFile:   activeFile,
		archFiles:    archFiles,
		config:       config,
		activeFileId: activeFileId,
		idxList:      skipList,
		meta:         meta,
		listIndex:    list.New(),
	}
}

//根据配置重新打开数据库
func Reopen(path string) (*MinDB, error) {
	if exist := utils.Exist(path + configSaveFile); !exist {
		return nil, ErrCfgNotExist
	}

	var config Config

	if bytes, err := ioutil.ReadFile(path + configSaveFile); err != nil {
		return nil, err
	} else {
		if err := json.Unmarshal(bytes, &config); err != nil { // 解码json格式的配置文件
			return nil, err
		}
	}
	return Open(config)
}

//关闭数据库，保存相关配置
func (db *MinDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.saveConfig(); err != nil {
		return err
	}

	if err := db.saveIndexes(); err != nil {
		return err
	}

	if err := db.saveMeta(); err != nil {
		return err
	}

	db.activeFile = nil
	db.idxList = nil
	return nil
}

//数据持久化
func (db *MinDB) Sync() error {
	if db == nil || db.activeFile == nil {
		return nil
	}

	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.activeFile.Sync()
}

//新增数据，如果存在则更新
func (db *MinDB) Set(key, value []byte) error {

	if err := db.checkKeyValue(key, value); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	e := storage.NewEntry(key, value) // 封装一个entry

	config := db.config

	// 如果数据文件空间不够，则关闭该文件，并新打开一个文件
	if db.activeFile.Offset+int64(e.Size()) > config.BlockSize {
		if err := db.activeFile.Close(true); err != nil { // 关闭当前activeFile
			return err
		}

		//保存旧的active文件到archFiles中
		db.archFiles[db.activeFileId] = db.activeFile

		activeFileId := db.activeFileId + 1 // 新的activeFileId在之前文件id上加一
		if dbFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize); err != nil {
			return err
		} else { // 新建一个文件
			db.activeFile = dbFile
			db.activeFileId = activeFileId
			db.meta.ActiveWriteOff = 0
		}
	}

	//如果key已经存在，则原来的值被舍弃，所以需要新增可回收的磁盘空间值
	if e := db.idxList.Get(key); e != nil {
		item := e.Value().(*index.Indexer)
		if item != nil {
			db.meta.UnusedSpace += uint64(item.EntrySize) // 新增可回收的磁盘空间值
		}
	}

	//数据索引
	idx := &index.Indexer{
		Key:       key,
		FileId:    db.activeFileId,
		EntrySize: e.Size(),
		Offset:    db.activeFile.Offset,
		KeySize:   uint32(len(key)),
	}

	//写入数据（entry）至文件中
	if err := db.activeFile.Write(e); err != nil {
		return err
	}
	db.meta.ActiveWriteOff = db.activeFile.Offset // 更新meta中的写offset

	//数据持久化
	if config.Sync {
		if err := db.activeFile.Sync(); err != nil {
			return err
		}
	}

	if config.IdxMode == KeyValueRamMode { // 如果开启了key value都在内存中的模式就把value也放在索引中
		idx.Value = value
		idx.ValueSize = uint32(len(value))
	}

	//存储索引至内存的索引结构中
	db.idxList.Put(key, idx)

	return nil
}

// 如果key存在，则将value追加至原来的value末尾 如果key不存在，则相当于Set方法
func (db *MinDB) Append(key, value []byte) error {

	if err := db.checkKeyValue(key, value); err != nil {
		return err
	}

	e, err := db.Get(key)
	if err != nil {
		return err
	}

	if e != nil {
		e = append(e, value...)
	} else {
		e = value
	}

	return db.Set(key, e)
}

func (db *MinDB) Get(key []byte) ([]byte, error) {
	keySize := uint32(len(key))
	if keySize == 0 {
		return nil, ErrEmptyKey
	}

	node := db.idxList.Get(key) // 在索引表中查找目标key所对应的节点
	if node == nil {
		return nil, ErrKeyNotExist
	}

	idx := node.Value().(*index.Indexer) // 类型断言为 indexer
	if idx == nil {
		return nil, ErrNilIndexer
	}

	db.mu.RLock() // 加读锁进行操作
	defer db.mu.RUnlock()

	//如果key和value均在内存中，则取内存中的value
	if db.config.IdxMode == KeyValueRamMode {
		return idx.Value, nil
	}

	//如果只有key在内存中，那么需要从db file中获取value
	if db.config.IdxMode == KeyOnlyRamMode {
		df := db.activeFile
		if idx.FileId != db.activeFileId {
			df = db.archFiles[idx.FileId]
		}
		if e, err := df.Read(idx.Offset, int64(idx.EntrySize)); err != nil { // 根据offset和size进行读取
			return nil, err
		} else {
			return e.Value, nil
		}
	}

	return nil, ErrKeyNotExist
}

//删除数据
func (db *MinDB) Remove(key []byte) error {

	if err := db.checkKeyValue(key, nil); err != nil {
		return err
	}

	//增加可回收的磁盘空间
	e := db.idxList.Get(key) // 获取到索引信息
	if e != nil {
		idx := e.Value().(*index.Indexer)
		if idx != nil {
			db.meta.UnusedSpace += uint64(idx.EntrySize) //更新可回收的磁盘空间
		}
	}

	//删除其在内存中的索引
	if e != nil {
		db.idxList.Remove(key)
	}
	return nil
}

//重新组织磁盘中的数据，回收磁盘空间
func (db *MinDB) Reclaim() error {

	if db.meta.UnusedSpace < db.config.ReclaimThreshold {
		return ErrReclaimUnreached
	}

	if db.idxList.Len <= 0 {
		return nil
	}

	//新建临时目录，用于暂存新的数据文件
	reclaimPath := db.config.DirPath + reclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil {
		return err
	}

	defer os.RemoveAll(reclaimPath)

	var (
		success             = true
		activeFileId uint32 = 0
		newArchFiles        = make(ArchivedFiles)
		df           *storage.DBFile
	)

	//遍历所有的key，将数据写入到临时文件中
	db.idxList.Foreach(func(e *index.Element) bool {
		idx := e.Value().(*index.Indexer) // 得到索引信息

		if idx != nil && db.archFiles[idx.FileId] != nil { // 如果该文件存在
			if df == nil { // 如果是第一次遍历，df尚未初始化
				df, _ = storage.NewDBFile(reclaimPath, activeFileId, db.config.RwMethod, db.config.BlockSize)
				newArchFiles[activeFileId] = df // 将新建的数据文件放入暂时的封存文件映射中
			}

			if int64(idx.EntrySize)+df.Offset > db.config.BlockSize { // 如果当前数据文件放不下当前遍历到的索引
				df.Close(true)    // 关闭当前数据文件
				activeFileId += 1 // 文件id 加一

				df, _ = storage.NewDBFile(reclaimPath, activeFileId, db.config.RwMethod, db.config.BlockSize)
				newArchFiles[activeFileId] = df // 新建一个数据文件
			}

			entry, err := db.archFiles[idx.FileId].Read(idx.Offset, int64(idx.EntrySize)) // 读取当前索引对应的entry
			if err != nil {
				success = false
				return false
			}

			//更新索引
			idx.FileId = df.Id
			idx.Offset = df.Offset
			e.SetValue(idx)

			if err := df.Write(entry); err != nil { // 将entry写入到新的数据文件中
				success = false
				return false
			}
		}

		return true
	})

	db.mu.Lock()
	defer db.mu.Unlock()

	//重新保存索引
	if err := db.saveIndexes(); err != nil {
		return err
	}

	if success {

		//旧数据删除，临时目录拷贝为新的数据文件
		for _, v := range db.archFiles {
			os.Remove(v.File.Name())
		}

		for _, v := range newArchFiles {
			name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatName, v.Id)
			os.Rename(reclaimPath+name, db.config.DirPath+name)
		}

		//更新数据库相关信息
		db.meta.UnusedSpace = 0
		db.archFiles = newArchFiles
	}

	return nil
}

//复制数据库目录，用于备份
func (db *MinDB) Backup(dir string) (err error) {
	if utils.Exist(db.config.DirPath) {
		err = utils.CopyDir(db.config.DirPath, dir)
	}

	return err
}

// 检查key value是否符合规范
func (db *MinDB) checkKeyValue(key, value []byte) error {
	keySize := uint32(len(key))
	if keySize == 0 {
		return ErrEmptyKey
	}

	config := db.config
	if keySize > config.MaxKeySize {
		return ErrKeyTooLarge
	}

	valueSize := uint32(len(value))
	if valueSize > config.MaxValueSize {
		return ErrValueTooLarge
	}

	return nil
}

//关闭数据库之前保存配置
func (db *MinDB) saveConfig() (err error) {
	//保存配置
	path := db.config.DirPath + configSaveFile
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)

	bytes, err := json.Marshal(db.config)
	_, err = file.Write(bytes)
	err = file.Close()

	return
}

// 持久化索引状态
func (db *MinDB) saveIndexes() error {
	idxPath := db.config.DirPath + indexSaveFile
	return index.Store(db.idxList, idxPath)
}

// 持久化数据库信息
func (db *MinDB) saveMeta() error {
	metaPath := db.config.DirPath + dbMetaSaveFile
	return db.meta.Store(metaPath)
}
