package mindb

import (
	"mindb/ds/set"
	"mindb/storage"
	"sync"
)

//集合的相关操作接口

// SetIdx the set idx
type SetIdx struct {
	mu      sync.RWMutex
	indexes *set.Set
}

func newSetIdx() *SetIdx {
	return &SetIdx{indexes: set.New()}
}

// SAdd 添加元素，返回添加后的集合中的元素个数
func (db *MinDB) SAdd(key []byte, members ...[]byte) (res int, err error) {

	if err = db.checkKeyValue(key, members...); err != nil {
		return
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	for _, m := range members {
		e := storage.NewEntryNoExtra(key, m, Set, SetSAdd)
		if err = db.store(e); err != nil {
			return
		}

		res = db.setIndex.indexes.SAdd(string(key), m)
	}

	return
}

// SPop 随机移除并返回集合中的count个元素
func (db *MinDB) SPop(key []byte, count int) (values [][]byte, err error) {

	if err = db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	values = db.setIndex.indexes.SPop(string(key), count)
	for _, v := range values {
		e := storage.NewEntryNoExtra(key, v, Set, SetSRem)
		if err = db.store(e); err != nil {
			return
		}
	}

	return
}

// SIsMember 判断 member 元素是不是集合 key 的成员
func (db *MinDB) SIsMember(key, member []byte) bool {

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	return db.setIndex.indexes.SIsMember(string(key), member)
}

// SRandMember 从集合中返回随机元素，count的可选值如下：
//如果 count 为正数，且小于集合元素数量，则返回一个包含 count 个元素的数组，数组中的元素各不相同
//如果 count 大于等于集合元素数量，那么返回整个集合
//如果 count 为负数，则返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为 count 的绝对值
func (db *MinDB) SRandMember(key []byte, count int) [][]byte {

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	return db.setIndex.indexes.SRandMember(string(key), count)
}

// SRem 移除集合 key 中的一个或多个 member 元素，不存在的 member 元素会被忽略
//被成功移除的元素的数量，不包括被忽略的元素
func (db *MinDB) SRem(key []byte, members ...[]byte) (res int, err error) {

	if err = db.checkKeyValue(key, members...); err != nil {
		return
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	for _, m := range members {
		if ok := db.setIndex.indexes.SRem(string(key), m); ok {
			e := storage.NewEntryNoExtra(key, m, Set, SetSRem)
			if err = db.store(e); err != nil {
				return
			}

			res++
		}
	}
	return
}

// SMove 将 member 元素从 src 集合移动到 dst 集合
func (db *MinDB) SMove(src, dst, member []byte) error {

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if ok := db.setIndex.indexes.SMove(string(src), string(dst), member); ok {
		e := storage.NewEntry(src, member, dst, Set, SetSMove)
		if err := db.store(e); err != nil {
			return err
		}
	}

	return nil
}

// SCard 返回集合中的元素个数
func (db *MinDB) SCard(key []byte) int {

	if err := db.checkKeyValue(key, nil); err != nil {
		return 0
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	return db.setIndex.indexes.SCard(string(key))
}

// SMembers 返回集合中的所有元素
func (db *MinDB) SMembers(key []byte) (val [][]byte) {

	if err := db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	return db.setIndex.indexes.SMembers(string(key))
}

// SUnion 返回给定全部集合数据的并集
func (db *MinDB) SUnion(keys ...[]byte) (val [][]byte) {

	if keys == nil || len(keys) == 0 {
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	var s []string
	for _, k := range keys {
		s = append(s, string(k))
	}

	return db.setIndex.indexes.SUnion(s...)
}

//  SDiff 返回给定集合数据的差集
func (db *MinDB) SDiff(keys ...[]byte) (val [][]byte) {

	if keys == nil || len(keys) == 0 {
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	var s []string
	for _, k := range keys {
		s = append(s, string(k))
	}

	return db.setIndex.indexes.SDiff(s...)
}
