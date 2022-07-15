package mindb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mindb/index"
	"mindb/storage"
	"mindb/utils"
	"os"
	"sync"
	"time"
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

	ErrInvalidTTL = errors.New("mindb: invalid ttl")

	ErrKeyExpired = errors.New("mindb: key is expired")
)

const (
	//保存配置的文件名称
	configSaveFile = string(os.PathSeparator) + "db.cfg"

	////保存索引状态的文件名称
	//indexSaveFile = string(os.PathSeparator) + "db.idx"

	//保存数据库相关信息的文件名称
	dbMetaSaveFile = string(os.PathSeparator) + "db.meta"

	//回收磁盘空间时的临时目录
	reclaimPath = string(os.PathSeparator) + "mindb_reclaim"

	// ExtraSeparator 额外信息的分隔符，用于存储一些额外的信息（因此一些操作的value中不能包含此分隔符）
	ExtraSeparator = "\\0"

	//保存过期字典的文件名称
	expireFile = string(os.PathSeparator) + "db.expires"
)

type (
	MinDB struct {
		activeFile    ActiveFiles     //当前活跃文件
		activeFileIds ActiveFileIds   //活跃文件id
		archFiles     ArchivedFiles   //已封存文件
		strIndex      *StrIdx         //字符串索引列表
		listIndex     *ListIdx        //list索引列表
		hashIndex     *HashIdx        //hash索引列表
		setIndex      *SetIdx         //集合索引列表
		zsetIndex     *ZsetIdx        //有序集合索引列表
		config        Config          //数据库配置
		mu            sync.RWMutex    //mutex
		meta          *storage.DBMeta //数据库配置额外信息
		expires       storage.Expires //过期字典
	}

	// ActiveFiles 不同类型的当前活跃文件
	ActiveFiles map[DataType]*storage.DBFile

	ActiveFileIds map[DataType]uint32

	// ArchivedFiles 不同类型的已封存的文件map索引
	ArchivedFiles map[DataType]map[uint32]*storage.DBFile
)

// Open 打开一个数据库实例
func Open(config Config) (*MinDB, error) {

	//如果目录不存在则创建
	if !utils.Exist(config.DirPath) {
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//加载数据文件
	archFiles, activeFileIds, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	// 加载活跃文件
	activeFiles := make(ActiveFiles)
	for dataType, fileId := range activeFileIds {
		file, err := storage.NewDBFile(config.DirPath, fileId, config.RwMethod, config.BlockSize, dataType)
		if err != nil {
			return nil, err
		}
		activeFiles[dataType] = file
	}

	//加载过期字典
	expires := storage.LoadExpires(config.DirPath + expireFile)

	//加载数据库额外信息（meta）
	meta, _ := storage.LoadMeta(config.DirPath + dbMetaSaveFile)

	// 更新当前活跃文件的写偏移
	for dataType, file := range activeFiles {
		file.Offset = meta.ActiveWriteOff[dataType]
	}

	db := &MinDB{
		activeFile:    activeFiles,
		activeFileIds: activeFileIds,
		archFiles:     archFiles,
		config:        config,
		meta:          meta,
		strIndex:      newStrIdx(),
		listIndex:     newListIdx(),
		hashIndex:     newHashIdx(),
		setIndex:      newSetIdx(),
		zsetIndex:     newZsetIdx(),
		expires:       expires,
	}

	// 从文件中加载索引信息
	if err := db.loadIdxFromFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Reopen 根据配置重新打开数据库
func Reopen(path string) (*MinDB, error) {
	if exist := utils.Exist(path + configSaveFile); !exist {
		return nil, ErrCfgNotExist
	}

	var config Config

	bytes, err := ioutil.ReadFile(path + configSaveFile)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(bytes, &config); err != nil {
		return nil, err
	}
	return Open(config)
}

// Close 关闭数据库，保存相关配置
func (db *MinDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.saveConfig(); err != nil {
		return err
	}

	if err := db.saveMeta(); err != nil {
		return err
	}

	if err := db.expires.SaveExpires(db.config.DirPath + expireFile); err != nil { // 保存过期信息
		return err
	}

	// close and sync the active file
	for _, file := range db.activeFile {
		if err := file.Close(true); err != nil {
			return err
		}
	}

	// close the archived files.
	for _, archFile := range db.archFiles {
		for _, file := range archFile {
			if err := file.Sync(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Sync 数据持久化
func (db *MinDB) Sync() error {
	if db == nil || db.activeFile == nil {
		return nil
	}

	db.mu.RLock()
	defer db.mu.RUnlock()
	for _, file := range db.activeFile {
		if err := file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Reclaim 重新组织磁盘中的数据，回收磁盘空间
// Currently reclaim will block read operation of String in KeyOnlyMemMode.
// Because we must get value from db files, so you can execute it while closing the db.
func (db *MinDB) Reclaim() (err error) {

	var reclaimable bool
	for _, archFiles := range db.archFiles {
		if len(archFiles) >= db.config.ReclaimThreshold {
			reclaimable = true
			break
		}
	}
	if !reclaimable {
		return ErrReclaimUnreached
	}

	//新建临时目录，用于暂存新的数据文件
	reclaimPath := db.config.DirPath + reclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil {
		return err
	}

	defer os.RemoveAll(reclaimPath)

	db.mu.Lock()
	defer db.mu.Unlock()

	// 用goroutine处理不同类型的文件
	newArchivedFiles := sync.Map{}
	reclaimedTypes := sync.Map{}
	wg := sync.WaitGroup{}
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func(dType uint16) {
			defer func() {
				wg.Done()
			}()

			if len(db.archFiles[dType]) < db.config.ReclaimThreshold {
				newArchivedFiles.Store(dType, db.archFiles[dType])
				return
			}

			var (
				df        *storage.DBFile
				fileId    uint32
				archFiles = make(map[uint32]*storage.DBFile)
			)
			for _, file := range db.archFiles[dType] {
				var offset int64 = 0
				var reclaimEntries []*storage.Entry

				// read all entries in db file, and find the valid entry.
				for {
					if e, err := file.Read(offset); err == nil {
						if db.validEntry(e, offset, file.Id) {
							reclaimEntries = append(reclaimEntries, e)
						}
						offset += int64(e.Size())
					} else {
						if err == io.EOF {
							break
						}
						log.Fatalf("err occurred when read the entry: %+v", err)
						return
					}
				}

				// rewrite the valid entries to new db file.
				for _, entry := range reclaimEntries {
					if df == nil || int64(entry.Size())+df.Offset > db.config.BlockSize {
						df, err = storage.NewDBFile(reclaimPath, fileId, db.config.RwMethod, db.config.BlockSize, dType)
						if err != nil {
							log.Fatalf("err occurred when create new db file: %+v", err)
							return
						}
						archFiles[fileId] = df
						fileId += 1
					}

					if err = df.Write(entry); err != nil {
						log.Fatalf("err occurred when write the entry: %+v", err)
						return
					}

					//字符串类型的数据需要在这里更新索引
					// Since the str types value will be read from db file, so should update the index info.
					if dType == String {
						item := db.strIndex.idxList.Get(entry.Meta.Key)
						idx := item.Value().(*index.Indexer)
						idx.Offset = df.Offset - int64(entry.Size())
						idx.FileId = fileId
						db.strIndex.idxList.Put(idx.Meta.Key, idx)
					}
				}
			}
			reclaimedTypes.Store(dType, struct{}{})
			newArchivedFiles.Store(dType, archFiles)
		}(uint16(i))
	}
	wg.Wait()

	dbArchivedFiles := make(ArchivedFiles)
	for i := 0; i < 5; i++ {
		dType := uint16(i)
		value, ok := newArchivedFiles.Load(dType)
		if !ok {
			log.Printf("one type of data(%d) is missed after reclaiming.", dType)
			return
		}
		dbArchivedFiles[dType] = value.(map[uint32]*storage.DBFile)
	}

	//delete the old db files.
	for dataType, files := range db.archFiles {
		if _, exist := reclaimedTypes.Load(dataType); exist {
			for _, f := range files {
				_ = os.Remove(f.File.Name())
			}
		}
	}

	//copy the temporary reclaim directory as new db files.
	for dataType, files := range dbArchivedFiles {
		if _, exist := reclaimedTypes.Load(dataType); exist {
			for _, f := range files {
				name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[dataType], f.Id)
				os.Rename(reclaimPath+name, db.config.DirPath+name)
			}
		}
	}

	db.archFiles = dbArchivedFiles
	return

}

// Backup 复制数据库目录，用于备份
func (db *MinDB) Backup(dir string) (err error) {
	if utils.Exist(db.config.DirPath) {
		err = utils.CopyDir(db.config.DirPath, dir)
	}

	return
}

// 检查key value是否符合规范
func (db *MinDB) checkKeyValue(key []byte, value ...[]byte) error {
	keySize := uint32(len(key))
	if keySize == 0 {
		return ErrEmptyKey
	}

	config := db.config
	if keySize > config.MaxKeySize {
		return ErrKeyTooLarge
	}

	for _, v := range value {
		if uint32(len(v)) > config.MaxValueSize {
			return ErrValueTooLarge
		}
	}

	return nil
}

//关闭数据库之前保存配置
func (db *MinDB) saveConfig() (err error) {
	//保存配置
	path := db.config.DirPath + configSaveFile
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)

	bytes, err := json.Marshal(db.config)
	_, err = file.Write(bytes)
	err = file.Close()

	return
}

// 持久化数据库信息
func (db *MinDB) saveMeta() error {
	metaPath := db.config.DirPath + dbMetaSaveFile
	return db.meta.Store(metaPath)
}

// 建立索引
func (db *MinDB) buildIndex(e *storage.Entry, idx *index.Indexer) error {

	if db.config.IdxMode == KeyValueRamMode { // 如果开启了key value都在内存中的模式就把value也放在索引中
		idx.Meta.Value = e.Meta.Value
		idx.Meta.ValueSize = uint32(len(e.Meta.Value))
	}
	switch e.Type {
	case storage.String: // 如果是string，就把当前索引加入到跳表中
		db.buildStringIndex(idx, e.Mark)
	case storage.List: // 如果是list，就建立list索引
		db.buildListIndex(idx, e.Mark)
	case storage.Hash:
		db.buildHashIndex(idx, e.Mark)
	case storage.Set:
		db.buildSetIndex(idx, e.Mark)
	case storage.ZSet:
		db.buildZsetIndex(idx, e.Mark)
	}

	return nil
}

// 写数据
func (db *MinDB) store(e *storage.Entry) error {

	//如果数据文件空间不够，则持久化该文件，并新打开一个文件
	config := db.config
	if db.activeFile[e.Type].Offset+int64(e.Size()) > config.BlockSize {
		if err := db.activeFile[e.Type].Sync(); err != nil {
			return err
		}

		//保存旧的文件
		activeFileId := db.activeFileIds[e.Type]
		db.archFiles[e.Type][activeFileId] = db.activeFile[e.Type]
		activeFileId = activeFileId + 1

		newDbFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize, e.Type)
		if err != nil {
			return err
		}
		db.activeFile[e.Type] = newDbFile
		db.activeFileIds[e.Type] = activeFileId
		db.meta.ActiveWriteOff[e.Type] = 0
	}
	//
	////如果key已经存在，则原来的值被舍弃，所以需要新增可回收的磁盘空间值
	//if e := db.idxList.Get(e.Meta.Key); e != nil {
	//	item := e.Value().(*index.Indexer)
	//	if item != nil {
	//		db.meta.UnusedSpace += uint64(item.EntrySize)
	//	}
	//}

	// 写入entry至文件中
	if err := db.activeFile[e.Type].Write(e); err != nil {
		return err
	}

	db.meta.ActiveWriteOff[e.Type] = db.activeFile[e.Type].Offset

	// 数据持久化
	if config.Sync {
		if err := db.activeFile[e.Type].Sync(); err != nil {
			return err
		}
	}

	return nil
}

// 判断entry所属的操作标识(增、改类型的操作)，以及val是否是有效的
func (db *MinDB) validEntry(e *storage.Entry, offset int64, fileId uint32) bool {

	if e == nil {
		return false
	}

	mark := e.Mark
	switch e.Type {
	case String:
		if mark == StringSet { // 和当前索引的内容进行比较
			// expired key is not valid.
			now := uint32(time.Now().Unix())
			if deadline, exist := db.expires[string(e.Meta.Key)]; exist && deadline <= now {
				return false
			}

			// check the data position.
			node := db.strIndex.idxList.Get(e.Meta.Key)
			if node == nil {
				return false
			}
			indexer := node.Value().(*index.Indexer)
			if bytes.Compare(indexer.Meta.Key, e.Meta.Key) == 0 {
				if indexer == nil || indexer.FileId != fileId || indexer.Offset != offset {
					return false
				}
			}

			if val, err := db.Get(e.Meta.Key); err == nil && string(val) == string(e.Meta.Value) {
				return true
			}
		}
	case List:
		if mark == ListLPush || mark == ListRPush || mark == ListLInsert || mark == ListLSet {
			// TODO 由于List是链表结构，无法有效的进行检索，取出全部数据依次比较的开销太大
			if db.LValExists(e.Meta.Key, e.Meta.Value) {
				return true
			}
		}
	case Hash:
		if mark == HashHSet {
			if val := db.HGet(e.Meta.Key, e.Meta.Extra); string(val) == string(e.Meta.Value) {
				return true
			}
		}
	case Set:
		if mark == SetSMove {
			if db.SIsMember(e.Meta.Extra, e.Meta.Value) {
				return true
			}
		}

		if mark == SetSAdd {
			if db.SIsMember(e.Meta.Key, e.Meta.Value) {
				return true
			}
		}
	case ZSet:
		if mark == ZSetZAdd {
			if val, err := utils.StrToFloat64(string(e.Meta.Extra)); err == nil {
				score := db.ZScore(e.Meta.Key, e.Meta.Value)
				if score == val {
					return true
				}
			}
		}
	}

	return false
}
