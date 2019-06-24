package cache

import (
	"sync"
)

type call struct {
	wg  *sync.WaitGroup
	kvs map[string][]byte // can be interface{}
	err error
}

type CallMap struct {
	mu sync.Mutex
	m  map[string]*call
}

// TODO
// DoGet change call.kvs to interface{}

// key_1  *call1
// key_2  *call1
// key_3  *call1
// key_4  *call2
// key_5  *call2
// key_6  *call3
// key_7  *call3
// 1. when get kvs:
// key_1
// key_2
// key_5
// key_11 new(call) = *call4
// start *call4 , wait for *call1 *call2, *call4
// 2. when get kvs:
// key_11
// wait for *call4
func (self *CallMap) DoMGet(kvsResult *CallRes, fn func(...string) (map[string][]byte, error), keys ...string) {
	// calls in map, need to wait
	callsToWait := make(map[*call]*call)
	// keys need to call, ignore duplicate
	keysNeedCall := make(map[string]string)
	// keys map
	keyCall := make(map[string]*call)

	self.mu.Lock()
	if self.m == nil {
		self.m = make(map[string]*call)
	}

	for _, k := range keys {
		if c, ok := self.m[k]; ok {
			callsToWait[c] = c
			keyCall[k] = c
		} else {
			keysNeedCall[k] = k
			keyCall[k] = nil
		}
	}

	// start new call
	if len(keysNeedCall) > 0 {
		var wg sync.WaitGroup
		wg.Add(1)
		c := new(call)
		c.wg = &wg
		for _, v := range keysNeedCall {
			self.m[v] = c
			keyCall[v] = c
		}
		callsToWait[c] = c

		// without duplicate
		var kss []string
		for _, v := range keysNeedCall {
			kss = append(kss, v)
		}

		go func(this *CallMap, ks ...string) {
			defer wg.Done()
			res, err := fn(ks...)

			this.mu.Lock()
			defer this.mu.Unlock()
			for _, k := range ks {
				if _, ok := this.m[k]; ok && this.m[k] != nil {
					this.m[k].kvs = res
					this.m[k].err = err
				}
			}
		}(self, kss...)
	}
	self.mu.Unlock()

	// wait for all calls to finish
	var allWaits sync.WaitGroup
	for _, v := range callsToWait {
		allWaits.Add(1)
		go func(wc *call, kc map[string]*call) {
			defer allWaits.Done()

			wc.wg.Wait()
			for ke, vu := range kc {
				if vu == wc {
					if _, ok := wc.kvs[ke]; ok && wc.err == nil {
						kvsResult.Set(ke, true, wc.kvs[ke], nil)
					} else {
						kvsResult.Set(ke, true, nil, wc.err)
					}
				}
			}
		}(v, keyCall)
	}
	allWaits.Wait()

	self.mu.Lock()
	for _, k := range keys {
		delete(self.m, k)
	}
	self.mu.Unlock()
}
