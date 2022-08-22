package index

import (
	"bytes"
	"math"
	"math/rand"
	"time"
)

//SkipList是跳表的实现，跳表是一个高效的可替代平衡二叉搜索树的数据结构
//它能够在O(log(n))的时间复杂度下进行插入、删除、查找操作
//跳表的具体解释可参考Wikipedia上的描述：https://zh.wikipedia.org/wiki/%E8%B7%B3%E8%B7%83%E5%88%97%E8%A1%A8

const (
	//跳表索引最大层数，可根据实际情况进行调整
	maxLevel    int     = 18
	probability float64 = 1 / math.E
)

//遍历节点的函数，bool返回值为false时遍历结束
type handleEle func(e *Element) bool

type (
	// Node 跳表节点
	Node struct {
		next []*Element // next指针，分层存储
	}

	// Element 跳表存储元素定义
	Element struct {
		Node
		key   []byte      // 存储key
		value interface{} // 存储value
	}

	// SkipList 跳表定义
	SkipList struct {
		Node
		maxLevel       int         // 最大层数
		Len            int         // 跳表长度
		randSource     rand.Source // 随机数生成
		probability    float64
		probTable      []float64
		prevNodesCache []*Node
	}
)

// NewSkipList 初始化一个空的跳表
func NewSkipList() *SkipList {
	return &SkipList{
		Node:           Node{next: make([]*Element, maxLevel)},
		prevNodesCache: make([]*Node, maxLevel),
		maxLevel:       maxLevel,
		randSource:     rand.New(rand.NewSource(time.Now().UnixNano())), // 生成随机数
		probability:    probability,
		probTable:      probabilityTable(probability, maxLevel),
	}
}

// Key 获得跳表元素 key
func (e *Element) Key() []byte {
	return e.key
}

// Value 获得跳表元素 value
func (e *Element) Value() interface{} {
	return e.value
}

// SetValue 设置跳表元素的value值
func (e *Element) SetValue(val interface{}) {
	e.value = val
}

// Next 跳表的第一层索引是原始数据，有序排列，可根据Next方法获取一个串联所有数据的链表
func (e *Element) Next() *Element {
	return e.next[0]
}

// Front 获取跳表头元素，获取到之后，可向后遍历得到所有的数据
//	e := list.Front()
//	for p := e; p != nil; p = p.Next() {
//		//do something with Element p
//	}
func (t *SkipList) Front() *Element {
	return t.next[0] // Node头结点组合在SkipList中，t.next实际上是对头结点的next
}

// Put 存储一个元素至跳表中，如果key已经存在，则会更新其对应的value
// 因此此跳表的实现暂不支持相同的key
func (t *SkipList) Put(key []byte, value interface{}) *Element {
	var element *Element
	prev := t.backNodes(key)   // 找出key节点在每一层索引应该放的位置的前一个节点

	if element = prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		element.value = value    // 如果key和prev的下一个节点的key相等，说明该key已存在，更新value返回即可
		return element
	}

	element = &Element{
		Node: Node{
			next: make([]*Element, t.randomLevel()),  // 初始化ele的next索引层
		},
		key:   key,
		value: value,
	}
 	// 当前key应该插入的位置已经确定，就在prev的下一个位置
	for i := range element.next {    // 遍历ele的所有索引层，建立节点前后联系
		element.next[i] = prev[i].next[i]
		prev[i].next[i] = element
	}

	t.Len++
	return element
}

// Get 根据 key 查找对应的 Element 元素
//未找到则返回nil
func (t *SkipList) Get(key []byte) *Element {
	var prev = &t.Node
	var next *Element

	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}

	if next != nil && bytes.Compare(next.key, key) <= 0 {
		return next
	}

	return nil
}

// Exist 判断跳表是否存在对应的key
func (t *SkipList) Exist(key []byte) bool {
	return t.Get(key) != nil
}

// Remove 根据key删除跳表中的元素，返回删除后的元素指针
func (t *SkipList) Remove(key []byte) *Element {
	prev := t.backNodes(key)

	if element := prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		for k, v := range element.next {
			prev[k].next[k] = v
		}

		t.Len--
		return element
	}

	return nil
}

// Foreach 遍历跳表中的每个元素
func (t *SkipList) Foreach(fun handleEle) {
	for p := t.Front(); p != nil; p = p.Next() {
		if ok := fun(p); !ok {
			break
		}
	}
}

// 找到key对应的前一个节点索引的信息，即key节点在每一层索引的前一个节点
func (t *SkipList) backNodes(key []byte) []*Node {
	var prev = &t.Node
	var next *Element

	prevs := t.prevNodesCache

	for i := t.maxLevel - 1; i >= 0; i-- { // 从最高层索引开始遍历
		next = prev.next[i] // 当前节点在第i层索引上的下一个节点

		for next != nil && bytes.Compare(key, next.key) > 0 { // 如果目标节点的key比next节点的key大
			prev = &next.Node   // 将prev放到next节点的位置上
			next = next.next[i] // next通过当前层的索引跳到下一个位置
		} // 循环跳出后，key节点应位于pre和next之间

		prevs[i] = prev // 将当前的prev节点缓存到跳表中的对应层上
	} // 到下一层继续寻找

	return prevs
}

// FindPrefix 找到第一个和前缀匹配的Element
func (t *SkipList) FindPrefix(prefix []byte) *Element {
	var prev = &t.Node
	var next *Element

	for i := t.maxLevel - 1; i >= 0; i-- {  // 从最高层开始找
		next = prev.next[i]

		for next != nil && bytes.Compare(prefix, next.key) > 0 {  // 找到前缀小于next节点的位置，继续下一层查找
			prev = &next.Node
			next = next.next[i]
		}
	}

	if next == nil {
		next = t.Front()
	}

	return next
}

// 生成索引随机层数
func (t *SkipList) randomLevel() (level int) {
	r := float64(t.randSource.Int63()) / (1 << 63)  // 生成一个[0, 1)的概率值

	level = 1
	for level < t.maxLevel && r < t.probTable[level] {  // 找到第一个prob小于 r 的层数
		level++
	}
	return
}

func probabilityTable(probability float64, maxLevel int) (table []float64) {
	for i := 1; i <= maxLevel; i++ {   // 将每一层的prob值设置为p的(层数减一)次方
		prob := math.Pow(probability, float64(i-1))
		table = append(table, prob)
	}
	return table
}
