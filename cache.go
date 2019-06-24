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
	mu     sync.RWMutex             // protect cache table
	table  map[string]*list.Element // cache table
	maxLen int

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
		source: source,

		list:   list.New(),
		table:  make(map[string]*list.Element, maxLen),
		maxLen: defaultMaxLen,

		Expiration:    time.Duration(exp) * time.Second,
		sourceTimeout: time.Duration(t) * time.Millisecond,
		needRefresh:   make(map[string]*Item),
		retries:       r,
		interval:      time.Duration(ri) * time.Second,
		callMap:       new(CallMap),
	}

	go c.refresh()
	return c, nil
}

func (c *Cache) Get(key string) ([]byte, Info) {
	info := Info{
		Cached:           len(c.table),
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

	c.mu.RLock()
	el, ok := c.table[key]
	if ok {
		if el == nil || el.Value == nil {
			// never should be here
			c.mu.RUnlock()
			b = nil
			return b, info
		}
		item, o := el.Value.(*Item)
		if !o {
			// never should be here
			c.mu.RUnlock()
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
			info.StatusElse += 1
			c.mu.RUnlock()
			b = nil
			return b, info
		}
	} else {
		unCached = append(unCached, key)
	}
	c.mu.RUnlock()

	// push to list front
	if len(pushFront) > 0 {
		go func(pf []*list.Element) {
			c.lmu.Lock()
			defer c.lmu.Unlock()
			for _, p := range pf {
				c.list.MoveToFront(p)
			}
		}(pushFront)
	}
	// set expiration
	if len(expired) > 0 {
		go func(es []*Item) {
			c.rMu.Lock()
			defer c.rMu.Unlock()
			for _, i := range es {
				if atomic.CompareAndSwapInt32((*int32)(&i.Status), int32(OK), int32(EXPIRED)) {
					c.needRefresh[i.Key] = i
				}
			}
		}(expired)
	}

	if len(unCached) <= 0 {
		return b, info
	}
	info.StatusUnCached += len(unCached)

	// get from source
	ctx, cancel := context.WithTimeout(context.TODO(), c.sourceTimeout)
	kc := NewCallRes(unCached...)
	go c.mergeGet(kc, cancel, unCached...)

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

func (c *Cache) MGet(keys ...string) (map[string][]byte, Info) {
	info := Info{
		Cached:           len(c.table),
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
	c.mu.RLock()
	for _, k := range keys {
		// preset
		kvs[k] = nil
		if k == "" {
			info.Empty += 1
			continue
		}

		el, ok := c.table[k]
		if ok {
			if el == nil || el.Value == nil {
				// never should be here
				continue
			}
			item, o := el.Value.(*Item)
			if !o {
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
		}
	}
	c.mu.RUnlock()

	// push to list front
	if len(pushFront) > 0 {
		go func(pf []*list.Element) {
			c.lmu.Lock()
			defer c.lmu.Unlock()
			for _, p := range pf {
				c.list.MoveToFront(p)
			}
		}(pushFront)
	}
	// set expiration
	if len(expired) > 0 {
		go func(es []*Item) {
			c.rMu.Lock()
			defer c.rMu.Unlock()
			for _, i := range es {
				if atomic.CompareAndSwapInt32((*int32)(&i.Status), int32(OK), int32(EXPIRED)) {
					c.needRefresh[i.Key] = i
				}
			}
		}(expired)
	}

	if len(unCached) <= 0 {
		return kvs, info
	}
	info.StatusUnCached += len(unCached)

	// get from source
	ctx, cancel := context.WithTimeout(context.TODO(), c.sourceTimeout)
	kc := NewCallRes(unCached...)
	go c.mergeGet(kc, cancel, unCached...)

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
		Cached:           len(c.table),
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
		// TODO new field
		return info, errors.New("nil byte array")
	}

	err := c.source.Set(key, value)
	if err != nil {
		var unCached []string
		var pushFront []*list.Element
		var expired []*Item

		c.mu.RLock()
		el, ok := c.table[key]
		if ok {
			if el == nil || el.Value == nil {
				// never should be here
				c.mu.RUnlock()
				return info, err
			}
			item, o := el.Value.(*Item)
			if !o {
				// never should be here
				c.mu.RUnlock()
				return info, err
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
				c.mu.RUnlock()
				return info, err
			}
		} else {
			unCached = append(unCached, key)
		}
		c.mu.RUnlock()

		// push to list front
		// TODO 呵呵，居然有一次拿1000+的，逼我加采样？
		if len(pushFront) > 0 && len(pushFront) <= 300 {
			go func(pf []*list.Element) {
				c.lmu.Lock()
				defer c.lmu.Unlock()
				for _, p := range pf {
					c.list.MoveToFront(p)
				}
			}(pushFront)
		}
		// set expiration
		if len(expired) > 0 {
			go func(es []*Item) {
				c.rMu.Lock()
				defer c.rMu.Unlock()
				for _, i := range es {
					if atomic.CompareAndSwapInt32((*int32)(&i.Status), int32(OK), int32(EXPIRED)) {
						c.needRefresh[i.Key] = i
					}
				}
			}(expired)
		}

		if len(unCached) <= 0 {
			return info, err
		}
		info.StatusUnCached += len(unCached)

		// get from source
		kc := NewCallRes(unCached...)
		go c.mergeGet(kc, nil, unCached...)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// set success, refresh cache
	if le, o := c.table[key]; o {
		// 本地已有数据
		if le == nil || le.Value == nil {
			// never should be here
			return info, nil
		}
		temp, is := c.table[key].Value.(*Item)
		if !is {
			return info, nil
		}
		temp.Value = value
		temp.UpdatedAt = time.Now()
		temp.Retries = 0
		temp.Key = key
		temp.Status = OK
	} else {
		// 本地没有数据
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
		c.table[key] = element
		for c.list.Len() > c.maxLen {
			el := c.list.Back()
			c.list.Remove(el)
			delete(c.table, el.Value.(*Item).Key)
		}
		c.lmu.Unlock()
	}
	info.StatusOk += 1
	return info, nil
}

func (c *Cache) mergeGet(kc *CallRes, cancel context.CancelFunc, keys ...string) {
	if cancel != nil {
		defer cancel()
	}

	c.callMap.DoMGet(kc, c.source.MGet, keys...)
	cc := kc.GetCopy()

	c.merge(cc)
}

func (c *Cache) merge(cr map[string]*resItem) {
	c.mu.Lock()
	defer c.mu.Unlock()
	needFresh := make([]*Item, 0)

	t := time.Now()
	// OK 状态的key，不会进入 c.needRefresh
	for key, item := range cr {
		// 远程数据失败
		if !item.done || item.err != nil {
			if _, o := c.table[key]; o {
				// 本地有数据
				// 无效的请求作废，不能作为更新本地状态的依据
				continue
			} else {
				// 本地没有的数据，没有请求成功
				it := &Item{
					Key:       key,
					Status:    MISS,
					Value:     nil,
					Retries:   1,
					CreatedAt: t,
					UpdatedAt: t,
				}

				c.lmu.Lock()
				element := c.list.PushFront(it)
				c.table[key] = element
				for c.list.Len() > c.maxLen {
					el := c.list.Back()
					c.list.Remove(el)
					delete(c.table, el.Value.(*Item).Key)
				}
				c.lmu.Unlock()

				needFresh = append(needFresh, it)
				continue
			}
		}

		// 远程数据成功
		if _, o := c.table[key]; o {
			// 本地已有数据
			temp, is := c.table[key].Value.(*Item)
			if !is {
				// never should be here
				continue
			}
			if item.value == nil {
				// 源数据不存在
				temp.Retries += 1
				temp.Status = MISS
				temp.Key = key
				temp.UpdatedAt = time.Now()
				temp.Value = nil
				if temp.Retries >= c.retries {
					c.lmu.Lock()
					c.list.Remove(c.table[key])
					c.lmu.Unlock()
					delete(c.table, key)
					continue
				}
				needFresh = append(needFresh, temp)
				continue
			} else {
				// EXPIRED -> OK
				// 本地和源数据都存在
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
				// 远程没有数据
				it := &Item{
					Key:       key,
					Status:    MISS,
					Value:     nil,
					Retries:   1,
					CreatedAt: t,
					UpdatedAt: t,
				}

				c.lmu.Lock()
				element := c.list.PushFront(it)
				c.table[key] = element
				for c.list.Len() > c.maxLen {
					el := c.list.Back()
					c.list.Remove(el)
					delete(c.table, el.Value.(*Item).Key)
				}
				c.lmu.Unlock()

				needFresh = append(needFresh, it)
				continue
			} else {
				// 远程有数据
				it := &Item{
					Key:       key,
					Status:    OK,
					Value:     item.value,
					Retries:   0,
					CreatedAt: t,
					UpdatedAt: t,
				}

				c.lmu.Lock()
				element := c.list.PushFront(it)
				c.table[key] = element
				for c.list.Len() > c.maxLen {
					el := c.list.Back()
					c.list.Remove(el)
					delete(c.table, el.Value.(*Item).Key)
				}
				c.lmu.Unlock()

				continue
			}
		}
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
				c.mergeGet(kc, nil, keys...)
			}
		}
	}
}

func (c *Cache) AddToRefresh(items []*Item) {
	go func(is []*Item) {
		c.rMu.Lock()
		defer c.rMu.Unlock()
		for _, i := range is {
			c.needRefresh[i.Key] = i
		}
	}(items)
}

func (c *Cache) Close() {
	c.source.Close()
}
