package cache

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestSingleFlight(t *testing.T) {
	kvs := map[string][]byte{
		"1":  []byte("hello1"),
		"2":  []byte("hello2"),
		"3":  []byte("hello3"),
		"4":  []byte("hello4"),
		"5":  []byte("hello5"),
		"6":  []byte("hello6"),
		"7":  []byte("hello7"),
		"8":  []byte("hello8"),
		"9":  []byte("hello9"),
		"10": []byte("hello10"),
	}

	g := new(CallMap)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(j int) {
			tRes := NewCallRes(strconv.Itoa(j), strconv.Itoa(j+1), strconv.Itoa(j+2), strconv.Itoa(j+3))
			defer wg.Done()
			g.DoMGet(tRes, func(s ...string) (map[string][]byte, error) {
				res := make(map[string][]byte)
				for _, k := range s {
					if v, ok := kvs[k]; ok {
						res[k] = v
					}
				}
				fmt.Println("send:", s)
				time.Sleep(time.Duration(100) * time.Millisecond)
				return res, nil
			}, strconv.Itoa(j), strconv.Itoa(j+1), strconv.Itoa(j+2), strconv.Itoa(j+3))
			for k, v := range tRes.GetCopy() {
				fmt.Println(j, k, v.done, v.err, string(v.value[:]))
			}
		}(i)
	}
	wg.Wait()
}
