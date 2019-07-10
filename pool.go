package cache

import (
	"sync"
	"time"
)

type Item struct {
	Key       string
	Value     []byte
	Status    status
	Retries   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

var itemPool = &sync.Pool{
	New: func() interface{} {
		return &Item{
			Key:     "",
			Value:   nil,
			Status:  OK,
			Retries: 0,
		}
	},
}

func newItem(key string, value []byte, status status, retries int, createdAt, updatedAt time.Time) *Item {
	item := itemPool.Get().(*Item)
	item.Key = key
	item.Value = value
	item.Status = status
	item.Retries = retries
	item.CreatedAt = createdAt
	item.UpdatedAt = updatedAt
	return item
}

func putItem(item *Item) {
	item.Value = nil
	itemPool.Put(item)
}
