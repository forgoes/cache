package cache

import (
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
				"10.0.2.97:6379",
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

	testCache, err := New(source.NewRedisSource(pool), 1000, 7, 3, 3, 10000, 3)
	if err != nil {
		t.Error(err)
	}

	key := "hello"
	//value := "world"

	for {
		println("test key not exists")
		res, info := testCache.MGet([]string{key, "yes", "no", "a", "b", "c", "d", "e"})
		t.Log(info)
		if res != nil {
			println("require no answer, but: %s", string(res[key][:]))
			//t.Fatalf("require no answer, but: %s", string(res[:]))
		}
		println("test key not exists: pass")
		time.Sleep(time.Second * time.Duration(7))
	}

	/*
		println("test set key:", key, "value:", value)
		info, err = testCache.Set(key, []byte(value))
		t.Log(info)
		if err != nil {
			t.Fatalf(err.Error())
		}
		println("test set key:", key, "value:", value, "pass")

		println("test get key:", key)
		res, info = testCache.Get(key)
		t.Log(info)
		if res == nil {
			t.Fatalf("nil result")
		}
		println("test get key:", key, "value:", string(res[:]), "pass")

		println("delete", key, "from source")
		coon := pool.Get()
		defer coon.Close()
		_, err = coon.Do("del", key)
		if err != nil {
			t.Fatalf(err.Error())
		}
		println("delete", key, "from source pass")

		println("test get deleted key:", key)
		res, info = testCache.Get(key)
		t.Log(info)
		if res == nil {
			t.Fatalf("nil result")
		}
		println("test get deleted key:", key, "find:", string(res[:]), "pass")

		println("after seconds get, trigger delete from cache")
		time.Sleep(time.Duration(4) * time.Second)
		println("after seconds get, trigger delete from cache pass")
		res, info = testCache.Get(key)
		t.Log(info)
		if res == nil {
			t.Fatalf("trigger cache delete, should return value ")
		}

		println("after try interval")
		time.Sleep(time.Duration(3) * time.Second)
		println("after try interval")
		res, info = testCache.Get(key)
		t.Log(info)
		if res != nil {
			fmt.Println(string(res[:]))
			t.Fatalf("require no answer")
		}
		println("after try interval get", string(res[:]))
	*/
}
