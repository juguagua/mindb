package mindb

import (
	"io"
	"mindb/ds/list"
	"mindb/index"
	"mindb/utils"
	"sort"
	"strconv"
	"strings"
	"time"
)

// 数据类型定义
type DataType = uint16

const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

// 字符串相关操作标识
const (
	StringSet uint16 = iota
	StringRem
)

//列表相关操作标识
const (
	ListLPush uint16 = iota
	ListRPush
	ListLPop
	ListRPop
	ListLRem
	ListLInsert
	ListLSet
	ListLTrim
)

//哈希相关操作标识
const (
	HashHSet uint16 = iota
	HashHDel
)

//集合相关操作标识
const (
	SetSAdd uint16 = iota
	SetSRem
	SetSMove
)

//有序集合相关操作标识
const (
	ZSetZAdd uint16 = iota
	ZSetZRem
)

//建立字符串索引
func (db *MinDB) buildStringIndex(idx *index.Indexer, opt uint16) {
	if db.listIndex == nil || idx == nil {
		return
	}

	now := uint32(time.Now().Unix())
	if deadline, exist := db.expires[string(idx.Meta.Key)]; exist && deadline <= now {
		return
	}

	switch opt {
	case StringSet:
		db.idxList.Put(idx.Meta.Key, idx)
	case StringRem:
		db.idxList.Remove(idx.Meta.Key)
	}
}

// 建立列表索引
func (db *MinDB) buildListIndex(idx *index.Indexer, opt uint16) {
	if db.listIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch opt { // 根据操作类型对列表执行相应操作
	case ListLPush:
		db.listIndex.LPush(key, idx.Meta.Value)
	case ListLPop:
		db.listIndex.LPop(key)
	case ListRPush:
		db.listIndex.RPush(key, idx.Meta.Value)
	case ListRPop:
		db.listIndex.RPop(key)
	case ListLRem:
		if count, err := strconv.Atoi(string(idx.Meta.Extra)); err == nil {
			db.listIndex.LRem(key, idx.Meta.Value, count)
		}
	case ListLInsert:
		extra := string(idx.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			pivot := []byte(s[0])
			if opt, err := strconv.Atoi(s[1]); err == nil {
				db.listIndex.LInsert(string(idx.Meta.Key), list.InsertOption(opt), pivot, idx.Meta.Value)
			}
		}
	case ListLSet:
		if i, err := strconv.Atoi(string(idx.Meta.Extra)); err == nil {
			db.listIndex.LSet(key, i, idx.Meta.Value)
		}
	case ListLTrim:
		extra := string(idx.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			start, _ := strconv.Atoi(s[0])
			end, _ := strconv.Atoi(s[1])

			db.listIndex.LTrim(string(idx.Meta.Key), start, end)
		}
	}
}

//建立哈希索引
func (db *MinDB) buildHashIndex(idx *index.Indexer, opt uint16) {

	if db.hashIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch opt {
	case HashHSet:
		db.hashIndex.HSet(key, string(idx.Meta.Extra), idx.Meta.Value)
	case HashHDel:
		db.hashIndex.HDel(key, string(idx.Meta.Extra))
	}
}

//建立集合索引
func (db *MinDB) buildSetIndex(idx *index.Indexer, opt uint16) {

	if db.hashIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch opt {
	case SetSAdd:
		db.setIndex.SAdd(key, idx.Meta.Value)
	case SetSRem:
		db.setIndex.SRem(key, idx.Meta.Value)
	case SetSMove:
		extra := idx.Meta.Extra
		db.setIndex.SMove(key, string(extra), idx.Meta.Value)
	}
}

//建立有序集合索引
func (db *MinDB) buildZsetIndex(idx *index.Indexer, opt uint16) {

	if db.hashIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch opt {
	case ZSetZAdd:
		if score, err := utils.StrToFloat64(string(idx.Meta.Extra)); err == nil {
			db.zsetIndex.ZAdd(key, score, string(idx.Meta.Value))
		}
	case ZSetZRem:
		db.zsetIndex.ZRem(key, string(idx.Meta.Value))
	}
}

// 从文件中加载String、List、Hash、Set、ZSet索引
func (db *MinDB) loadIdxFromFiles() error {
	if db.archFiles == nil && db.activeFile == nil {
		return nil
	}

	var fileIds []int
	dbFile := make(ArchivedFiles)
	for k, v := range db.archFiles { // 从db的archFiles中取出所有的archFiles
		dbFile[k] = v
		fileIds = append(fileIds, int(k))
	}

	dbFile[db.activeFileId] = db.activeFile
	fileIds = append(fileIds, int(db.activeFileId))

	sort.Ints(fileIds) // 对file id进行排序后遍历
	for i := 0; i < len(fileIds); i++ {
		fid := uint32(fileIds[i])
		df := dbFile[fid]
		var offset int64 = 0

		for offset <= db.config.BlockSize { // 读当前文件
			if e, err := df.Read(offset); err == nil { // 从当前数据文件中取出entry
				idx := &index.Indexer{ // 根据该entry构建索引结构
					Meta:      e.Meta,
					FileId:    fid,
					EntrySize: e.Size(),
					Offset:    offset,
				}
				offset += int64(e.Size()) // 更新offset

				if err := db.buildIndex(e, idx); err != nil { // 建立索引
					return err
				}
			} else {
				if err == io.EOF { // 该文件读完就退出
					break
				}

				return err
			}
		}
	}

	return nil
}
