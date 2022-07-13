package mindb

import (
	"bytes"
	"log"
	"mindb/ds/list"
	"mindb/storage"
	"strconv"
	"strings"
)

//---------列表相关操作接口-----------

// 在列表的头部添加元素，返回添加后的列表长度
func (db *MinDB) LPush(key []byte, values ...[]byte) (res int, err error) {
	if err := db.checkKeyValue(key, values...); err != nil {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	for _, val := range values {
		e := storage.NewEntryNoExtra(key, val, List, ListLPush) // 构建相应操作的entry

		if err = db.store(e); err != nil { // 将entry写入到active file中
			return
		}
		res = db.listIndex.LPush(string(key), val)
	}

	return
}

// 在列表的尾部添加元素，返回添加后的列表长度
func (db *MinDB) RPush(key []byte, values ...[]byte) (res int, err error) {
	if err := db.checkKeyValue(key, values...); err != nil {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	for _, val := range values {
		e := storage.NewEntryNoExtra(key, val, List, ListRPush)
		if err = db.store(e); err != nil {
			return
		}
		res = db.listIndex.RPush(string(key), val)
	}

	return
}

// 取出列表头部的元素
func (db *MinDB) LPop(key []byte) ([]byte, error) {

	db.mu.Lock()
	defer db.mu.Unlock()

	val := db.listIndex.LPop(string(key))

	if val != nil {
		e := storage.NewEntryNoExtra(key, val, List, ListLPop)
		if err := db.store(e); err != nil {
			log.Println("error occurred when store ListLPop data")
		}
	}

	return val, nil
}

// 取出列表尾部的元素
func (db *MinDB) RPop(key []byte) ([]byte, error) {

	db.mu.Lock()
	defer db.mu.Unlock()

	val := db.listIndex.RPop(string(key))

	if val != nil {
		e := storage.NewEntryNoExtra(key, val, List, ListRPop)
		if err := db.store(e); err != nil {
			log.Println("error occurred when store ListRPop data")
		}
	}

	return val, nil
}

// 返回列表在index处的值，如果不存在则返回nil
func (db *MinDB) LIndex(key []byte, idx int) []byte {

	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.listIndex.LIndex(string(key), idx)
}

// 根据参数 count 的值，移除列表中与参数 value 相等的元素
// count > 0 : 从表头开始向表尾搜索，移除与 value 相等的元素，数量为 count
// count < 0 : 从表尾开始向表头搜索，移除与 value 相等的元素，数量为 count 的绝对值
// count = 0 : 移除列表中所有与 value 相等的值
// 返回成功删除的元素个数
func (db *MinDB) LRem(key, value []byte, count int) (int, error) {

	db.mu.Lock()
	defer db.mu.Unlock()

	res := db.listIndex.LRem(string(key), value, count)

	if res > 0 {
		c := strconv.Itoa(count)
		e := storage.NewEntry(key, value, []byte(c), List, ListLRem)
		if err := db.store(e); err != nil {
			return res, err
		}

	}

	return res, nil
}

func (db *MinDB) LInsert(key string, option list.InsertOption, pivot, val []byte) error {

	if err := db.checkKeyValue([]byte(key), val); err != nil {
		return err
	}

	if strings.Contains(string(pivot), ExtraSeparator) {
		return ErrExtraContainsSeparator
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	res := db.listIndex.LInsert(key, option, pivot, val)
	if res != -1 {
		var buf bytes.Buffer
		buf.Write(pivot)
		buf.Write([]byte(ExtraSeparator))
		opt := strconv.Itoa(int(option))
		buf.Write([]byte(opt))

		e := storage.NewEntry([]byte(key), val, buf.Bytes(), List, ListLInsert)
		if _, err := db.store(e); err != nil {
			return err
		}
	}

	return nil
}

func (db *MinDB) LSet(key []byte, idx int, val []byte) error {

	if err := db.checkKeyValue(key, val); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if res := db.listIndex.LSet(string(key), idx, val); res {
		i := strconv.Itoa(idx)
		e := storage.NewEntry(key, val, []byte(i), List, ListLSet)
		if _, err := db.store(e); err != nil {
			return err
		}
	}

	return nil
}

func (db *MinDB) LTrim(key []byte, start, end int) error {

	db.mu.Lock()
	defer db.mu.Unlock()

	if res := db.listIndex.LTrim(string(key), start, end); res {
		var buf bytes.Buffer
		buf.Write([]byte(strconv.Itoa(start)))
		buf.Write([]byte(ExtraSeparator))
		buf.Write([]byte(strconv.Itoa(end)))

		e := storage.NewEntry(key, nil, buf.Bytes(), List, ListLTrim)
		if _, err := db.store(e); err != nil {
			return err
		}
	}

	return nil
}

func (db *MinDB) LRange(key []byte, start, end int) ([][]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if err := db.checkKeyValue(key, nil); err != nil {
		return nil, err
	}

	return db.listIndex.LRange(string(key), start, end), nil
}

func (db *MinDB) LLen(key []byte) int {

	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.listIndex.LLen(string(key))
}
