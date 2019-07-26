package source

import (
	"github.com/gomodule/redigo/redis"
)

type Redis struct {
	pool *redis.Pool
}

func NewRedisSource(pool *redis.Pool) *Redis {
	return &Redis{
		pool: pool,
	}
}

func (r *Redis) Get(key string) ([]byte, error) {
	conn := r.pool.Get()
	defer conn.Close()

	res, err := conn.Do("get", key)
	if err != nil {
		return nil, err
	} else {
		bv, e := redis.Bytes(res, nil)
		if e != nil {
			return nil, e
		}
		return bv, nil
	}
}

func (r *Redis) MGet(keys ...string) (map[string][]byte, error) {
	res := make(map[string][]byte)

	conn := r.pool.Get()
	defer conn.Close()

	var tv []interface{}
	for i := 0; i < len(keys); i++ {
		tv = append(tv, interface{}(keys[i]))
	}
	var tp interface{}
	tp, err := conn.Do("mget", tv...)
	if err != nil {
		return nil, err
	}

	for i, v := range tp.([]interface{}) {
		bv, err := redis.Bytes(v, nil)
		if err != nil {
			res[keys[i]] = nil
		}
		res[keys[i]] = bv
	}
	return res, nil
}

func (r *Redis) Set(key string, value []byte) error {
	conn := r.pool.Get()
	defer conn.Close()

	_, err := conn.Do("set", key, value)
	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) Close() {
	r.pool.Close()
}
