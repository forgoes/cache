package cache

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type status int32

const (
	OK status = iota
	EXPIRED
	MISS
)

const (
	defaultTimeout       = 42    // 获取源数据，默认42ms超时
	defaultRetries       = 7     // MISS状态重试次数
	defaultRetryInterval = 60    // MISS状态重试间隔 (s)
	defaultExpiration    = 420   // 数据过期时间 (s)
	defaultMaxLen        = 10000 // 默认最大缓存10000
)

type MGet func(keys ...string) (map[string][]byte, error)
type Get func(key string) ([]byte, error)
type MGetTraceWrapper func(MGet) MGet
type GetTraceWrapper func(Get) Get

type Source interface {
	Get(key string) ([]byte, error)
	MGet(keys ...string) (map[string][]byte, error)
	Set(key string, value []byte) error
	Close()
}

type Item struct {
	Key       string
	Value     []byte
	Status    status
	Retries   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Info struct {
	Cached           int
	Total            int
	Empty            int
	StatusOk         int
	StatusExpired    int
	StatusMiss       int
	StatusElse       int
	StatusUnCached   int
	MergeFromCallMap int
	TriggerExpired   int
	TriggerPushFront int
	Action           string
	Key              string
}

type Cache struct {
	lmu    sync.Mutex // protect list
	list   *list.List
	maxLen int

	table *sync.Map

	rMu         sync.RWMutex     // protect needRefresh
	needRefresh map[string]*Item // 需要请求源数据

	source        Source        // data source
	Expiration    time.Duration // 过期时间
	sourceTimeout time.Duration // 获取源数据超时时间 ms
	retries       int           // MISS 状态重试次数
	interval      time.Duration // 重试时间间隔 (s)

	callMap *CallMap
}

func New(source Source, sourceTimeout int64, retries int, retryInterval int, expiration int, maxLen int) (*Cache, error) {
	if source == nil {
		return nil, errors.New("nil source")
	}

	t := sourceTimeout
	if t <= 0 {
		t = defaultTimeout
	}

	r := retries
	if r < 0 {
		r = defaultRetries
	}

	ri := retryInterval
	if ri < 0 {
		ri = defaultRetryInterval
	}

	exp := expiration
	if exp <= 0 {
		exp = defaultExpiration
	}

	ml := maxLen
	if ml <= 0 {
		ml = defaultMaxLen
	}

	c := &Cache{
		list:   list.New(),
		maxLen: ml,

		table: new(sync.Map),

		needRefresh: make(map[string]*Item),

		source:        source,
		Expiration:    time.Duration(exp) * time.Second,
		sourceTimeout: time.Duration(t) * time.Millisecond,
		retries:       r,
		interval:      time.Duration(ri) * time.Second,

		callMap: new(CallMap),
	}

	go c.refresh()
	return c, nil
}

func (c *Cache) convertInterface(i interface{}) (*list.Element, *Item, error) {
	// never should be error
	el, ok := i.(*list.Element)
	if !ok {
		return nil, nil, errors.New("interface to *list.Element error")
	}

	if el == nil || el.Value == nil {
		return nil, nil, errors.New("nil *list.Element")
	}

	item, ok := el.Value.(*Item)
	if !ok {
		return nil, nil, errors.New("*list.Element.Value to *Item error")
	}

	if item == nil {
		return nil, nil, errors.New("nil *Item")
	}

	return el, item, nil
}

func (c *Cache) pushFront(pf []*list.Element) {
	c.lmu.Lock()
	defer c.lmu.Unlock()
	for _, p := range pf {
		c.list.MoveToFront(p)
	}
}

func (c *Cache) addExpired(items []*Item) {
	c.rMu.Lock()
	defer c.rMu.Unlock()
	for _, i := range items {
		if atomic.CompareAndSwapInt32((*int32)(&i.Status), int32(OK), int32(EXPIRED)) {
			c.needRefresh[i.Key] = i
		}
	}
}

func (c *Cache) addRefresh(keys []string) {
	c.rMu.Lock()
	defer c.rMu.Unlock()
	for _, key := range keys {
		c.needRefresh[key] = nil
	}
}

func (c *Cache) Get(key string, mts ...MGetTraceWrapper) ([]byte, Info) {
	info := Info{
		Cached:           c.list.Len(),
		Total:            1,
		Empty:            0,
		StatusOk:         0,
		StatusExpired:    0,
		StatusMiss:       0,
		StatusElse:       0,
		StatusUnCached:   0,
		MergeFromCallMap: 0,
		TriggerExpired:   0,
		TriggerPushFront: 0,
		Action:           "get",
		Key:              key,
	}
	if key == "" {
		info.Empty += 1
		return nil, info
	}

	// read in memory
	var b []byte
	var unCached []string
	var pushFront []*list.Element
	var expired []*Item

	iElement, ok := c.table.Load(key)
	if ok {
		el, item, err := c.convertInterface(iElement)
		if err != nil {
			b = nil
			return b, info
		}

		switch item.Status {
		case OK:
			b = item.Value
			pushFront = append(pushFront, el)
			info.TriggerPushFront += 1
			info.StatusOk += 1
			if time.Since(item.UpdatedAt) > c.Expiration {
				expired = append(expired, item)
				info.TriggerExpired += 1
			}
		case EXPIRED:
			b = item.Value
			pushFront = append(pushFront, el)
			info.TriggerPushFront += 1
			info.StatusExpired += 1
		case MISS:
			b = nil
			pushFront = append(pushFront, el)
			info.TriggerPushFront += 1
			info.StatusMiss += 1
		default:
			// never should be here
			b = nil
			info.StatusElse += 1
			return b, info
		}
	} else {
		unCached = append(unCached, key)
	}

	// push front
	if len(pushFront) > 0 {
		go c.pushFront(pushFront)
	}

	// set expiration
	if len(expired) > 0 {
		go c.addExpired(expired)
	}

	// hit the cache and return
	if len(unCached) <= 0 {
		return b, info
	}

	// get from source
	info.StatusUnCached += len(unCached)
	ctx, cancel := context.WithTimeout(context.TODO(), c.sourceTimeout)
	kc := NewCallRes(unCached...)
	go c.mergeGet(kc, cancel, unCached, mts...)

	<-ctx.Done()

	// still we get some data, copy and response these
	cr := kc.GetCopy()
	for k, v := range cr {
		if k == key && v != nil && v.err == nil && v.value != nil && v.done {
			b = v.value
			info.MergeFromCallMap += 1
		}
	}

	return b, info
}

func (c *Cache) MGet(keys []string, mts ...MGetTraceWrapper) (map[string][]byte, Info) {
	info := Info{
		Cached:           c.list.Len(),
		Total:            len(keys),
		Empty:            0,
		StatusOk:         0,
		StatusExpired:    0,
		StatusMiss:       0,
		StatusElse:       0,
		StatusUnCached:   0,
		MergeFromCallMap: 0,
		TriggerExpired:   0,
		TriggerPushFront: 0,
		Action:           "mget",
		Key:              "",
	}
	kvs := make(map[string][]byte)
	var unCached []string
	var pushFront []*list.Element
	var expired []*Item

	// read in memory
	for _, k := range keys {
		// preset
		kvs[k] = nil
		if k == "" {
			info.Empty += 1
			continue
		}

		iElement, ok := c.table.Load(k)
		if ok {
			el, item, err := c.convertInterface(iElement)
			if err != nil {
				// never should be here
				continue
			}

			switch item.Status {
			case OK:
				kvs[k] = item.Value
				pushFront = append(pushFront, el)
				info.TriggerPushFront += 1
				info.StatusOk += 1
				if time.Since(item.UpdatedAt) > c.Expiration {
					expired = append(expired, item)
					info.TriggerExpired += 1
				}
			case EXPIRED:
				kvs[k] = item.Value
				pushFront = append(pushFront, el)
				info.TriggerPushFront += 1
				info.StatusExpired += 1
			case MISS:
				kvs[k] = nil
				pushFront = append(pushFront, el)
				info.TriggerPushFront += 1
				info.StatusMiss += 1
			default:
				// never should be here
				info.StatusElse += 1
				continue
			}
		} else {
			unCached = append(unCached, k)
			continue
		}
	}

	// push to list front
	if len(pushFront) > 0 && len(pushFront) <= 280 {
		go c.pushFront(pushFront)
	} else {
		info.TriggerPushFront = 0
	}
	// set expiration
	if len(expired) > 0 {
		go c.addExpired(expired)
	}

	// all in cache and return
	if len(unCached) <= 0 {
		return kvs, info
	}

	// need get from source
	info.StatusUnCached += len(unCached)
	ctx, cancel := context.WithTimeout(context.TODO(), c.sourceTimeout)
	kc := NewCallRes(unCached...)
	go c.mergeGet(kc, cancel, unCached, mts...)

	<-ctx.Done()

	// still we get some data, copy and response these
	cr := kc.GetCopy()
	for k, v := range cr {
		if _, o := kvs[k]; o && v != nil && v.err == nil && v.value != nil && v.done {
			kvs[k] = v.value
			info.MergeFromCallMap += 1
		}
	}

	return kvs, info
}

func (c *Cache) Set(key string, value []byte) (Info, error) {
	info := Info{
		Cached:           c.list.Len(),
		Total:            1,
		Empty:            0,
		StatusOk:         0,
		StatusExpired:    0,
		StatusMiss:       0,
		StatusElse:       0,
		StatusUnCached:   0,
		MergeFromCallMap: 0,
		TriggerExpired:   0,
		TriggerPushFront: 0,
		Action:           "set",
		Key:              key,
	}
	if key == "" {
		info.Empty += 1
		return info, errors.New("empty key")
	}
	if value == nil {
		info.Empty += 1
		return info, errors.New("nil byte array")
	}

	err := c.source.Set(key, value)
	if err != nil {
		var unCached []string
		var pushFront []*list.Element
		var expired []*Item

		iElement, ok := c.table.Load(key)
		if ok {
			el, item, e := c.convertInterface(iElement)
			if e != nil {
				// never should be here
				return info, e
			}

			switch item.Status {
			case OK:
				pushFront = append(pushFront, el)
				info.TriggerPushFront += 1
				info.StatusOk += 1
				expired = append(expired, item)
				info.TriggerExpired += 1
			case EXPIRED:
				pushFront = append(pushFront, el)
				info.TriggerPushFront += 1
				info.StatusExpired += 1
			case MISS:
				pushFront = append(pushFront, el)
				info.TriggerPushFront += 1
				info.StatusMiss += 1
			default:
				// never should be here
				info.StatusElse += 1
				return info, err
			}
		} else {
			unCached = append(unCached, key)
		}

		// push to list front
		if len(pushFront) > 0 {
			go c.pushFront(pushFront)
		}
		// set expiration
		if len(expired) > 0 {
			go c.addExpired(expired)
		}

		// in cache and return
		if len(unCached) <= 0 {
			return info, err
		}

		// get from source and not wait
		info.StatusUnCached += len(unCached)
		kc := NewCallRes(unCached...)
		go c.mergeGet(kc, nil, unCached)
		return info, err
	} else {
		// set success, refresh cache
		iElement, ok := c.table.Load(key)
		if !ok {
			c.addOk(key, value)
			return info, nil
		} else {
			_, temp, e := c.convertInterface(iElement)
			if e != nil {
				// never should be here
				return info, e
			}
			// TODO push front
			temp.Value = value
			temp.UpdatedAt = time.Now()
			temp.Retries = 0
			temp.Key = key
			temp.Status = OK

			info.StatusOk += 1
			return info, nil
		}
	}
}

func (c *Cache) mergeGet(kc *CallRes, cancel context.CancelFunc, keys []string, mts ...MGetTraceWrapper) {
	if cancel != nil {
		defer cancel()
	}

	mGet := c.source.MGet
	for _, mt := range mts {
		mGet = mt(mGet)
	}

	c.callMap.DoMGet(kc, mGet, keys...)
	cc := kc.GetCopy()

	c.merge(cc)
}

func (c *Cache) addMiss(key string) *Item {
	t := time.Now()
	it := &Item{
		Key:       key,
		Status:    MISS,
		Value:     nil,
		Retries:   0,
		CreatedAt: t,
		UpdatedAt: t,
	}
	c.lmu.Lock()
	element := c.list.PushFront(it)
	for c.list.Len() > c.maxLen {
		el := c.list.Back()
		c.list.Remove(el)
		c.table.Delete(el.Value.(*Item).Key)
	}
	c.lmu.Unlock()
	c.table.Store(key, element)
	return it
}

func (c *Cache) addOk(key string, value []byte) {
	t := time.Now()
	it := &Item{
		Key:       key,
		Status:    OK,
		Value:     value,
		Retries:   0,
		CreatedAt: t,
		UpdatedAt: t,
	}

	c.lmu.Lock()
	element := c.list.PushFront(it)
	for c.list.Len() > c.maxLen {
		el := c.list.Back()
		c.list.Remove(el)
		c.table.Delete(el.Value.(*Item).Key)
	}
	c.lmu.Unlock()
	c.table.Store(key, element)
}

func (c *Cache) merge(cr map[string]*resItem) {
	needFresh := make([]string, 0)
	t := time.Now()

	for key, item := range cr {
		// 远程数据失败
		if !item.done || item.err != nil {
			_, ok := c.table.Load(key)
			if ok {
				// 远程数据失败，不足以修改本地已有数据的状态
				needFresh = append(needFresh, key)
				continue
			} else {
				// 远程数据失败，创建本地MISS数据
				c.addMiss(key)
				needFresh = append(needFresh, key)
				continue
			}
		}

		// 远程数据成功
		iElement, ok := c.table.Load(key)
		if ok {
			el, temp, e := c.convertInterface(iElement)
			if e != nil {
				// never should be here
				continue
			}

			if item.value == nil {
				// 远程没有数据，本地具备有效数据, 修改本地数据为无效
				temp.Retries += 1
				temp.Status = MISS
				temp.Key = key
				temp.UpdatedAt = t
				temp.Value = nil
				if temp.Retries >= c.retries {
					c.lmu.Lock()
					c.list.Remove(el)
					c.lmu.Unlock()
					c.table.Delete(key)
					continue
				}
				needFresh = append(needFresh, key)
				continue
			} else {
				// 远程有数据，本地也有数据
				temp.Value = item.value
				temp.UpdatedAt = time.Now()
				temp.Retries = 0
				temp.Key = key
				temp.Status = OK
				continue
			}
		} else {
			// 本地没有数据
			if item.value == nil {
				c.addMiss(key)
				needFresh = append(needFresh, key)
				continue
			} else {
				c.addOk(key, item.value)
				continue
			}
		}
	}

	if len(needFresh) > 0 {
		c.addRefresh(needFresh)
	}
}

func (c *Cache) refresh() {
	for {
		select {
		case <-time.After(c.interval):
			c.rMu.Lock()
			keys := make([]string, 0)
			for k := range c.needRefresh {
				keys = append(keys, k)
			}
			for k := range c.needRefresh {
				delete(c.needRefresh, k)
			}
			c.rMu.Unlock()

			if len(keys) > 0 {
				kc := NewCallRes(keys...)
				c.mergeGet(kc, nil, keys)
			}
		}
	}
}

func (c *Cache) Close() {
	c.source.Close()
}
