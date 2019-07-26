package cache

import (
	"sync"
)

type resItem struct {
	done  bool
	value []byte
	err   error
}

type CallRes struct {
	res map[string]*resItem
	mu  sync.RWMutex
}

func NewCallRes(keys ...string) *CallRes {
	res := make(map[string]*resItem)
	for _, k := range keys {
		if k == "" {
			continue
		} else {
			res[k] = &resItem{done: false, value: nil, err: nil}
		}
	}
	return &CallRes{res: res}
}

func (crs *CallRes) GetCopy() map[string]*resItem {
	crs.mu.RLock()
	defer crs.mu.RUnlock()
	cp := make(map[string]*resItem)
	for k, v := range crs.res {
		cp[k] = v
	}
	return cp
}

func (crs *CallRes) Set(key string, done bool, v []byte, err error) bool {
	crs.mu.Lock()
	defer crs.mu.Unlock()
	if _, ok := crs.res[key]; ok && crs.res[key] != nil {
		crs.res[key].done = done
		crs.res[key].value = v
		crs.res[key].err = err
		return true
	}
	return false
}
