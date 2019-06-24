package cache

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/govine/cache/source"
)

func TestMGet(t *testing.T) {
	connectTimeout := time.Duration(50) * time.Millisecond
	auth := ""
	pool := &redis.Pool{
		Dial: func() (c redis.Conn, err error) {
			c, err = redis.Dial(
				"tcp",
				"xxx.xxx.xxx.xxx:xxx",
				redis.DialConnectTimeout(connectTimeout),
				redis.DialReadTimeout(connectTimeout),
				redis.DialWriteTimeout(connectTimeout),
			)
			if err != nil {
				return nil, err
			}
			if auth != "" {
				if _, err := c.Do("AUTH", auth); err != nil {
					c.Close()
					return nil, err
				}
			}

			if _, err := c.Do("SELECT", 10); err != nil {
				c.Close()
				return nil, err
			}
			return
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		MaxIdle:     1024,
		MaxActive:   1024,
		IdleTimeout: time.Duration(30) * time.Millisecond,
	}

	testCache, err := New(source.NewRedisSource(pool), 42, 7, 3, 7, 10000)
	if err != nil {
		t.Error(err)
	}

	key := "hello"

	res, info := testCache.Get(key)
	t.Log(info)
	if res != nil {
		fmt.Println(string(res[:]))
		t.Error(errors.New("require no answer"))
	}
	fmt.Println(key, ":", string(res[:]))

	info, err = testCache.Set(key, []byte("world"))
	t.Log(info)
	if err != nil {
		t.Error(err)
	}

	res, info = testCache.Get(key)
	t.Log(info)
	if res == nil {
		t.Error(errors.New("nil result"))
	}
	fmt.Println(key, ":", string(res[:]))

	coon := pool.Get()
	defer coon.Close()

	_, err = coon.Do("del", key)
	if err != nil {
		t.Error(err)
	}

	res, info = testCache.Get(key)
	t.Log(info)
	if res == nil {
		t.Error(errors.New("nil result"))
	}

	time.Sleep(time.Duration(7) * time.Second)
	res, info = testCache.Get(key)
	t.Log(info)
	if res == nil {
		t.Error(errors.New("trigger cache delete, should return value "))
	}

	time.Sleep(time.Duration(7) * time.Second)
	res, info = testCache.Get(key)
	t.Log(info)
	if res != nil {
		fmt.Println(string(res[:]))
		t.Error(errors.New("require no answer"))
	}
}
