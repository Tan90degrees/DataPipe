package sharding

import (
	"context"
	"hash/fnv"

	"datapipe/internal/models"
)

type ShardID int

type Shard struct {
	ID      ShardID
	Data    []byte
	Index   int
	Total   int
	Key     string
	Meta    map[string]interface{}
}

type Sharder interface {
	Shard(ctx context.Context, data []byte, numShards int) ([]*Shard, error)
	ShardKey(ctx context.Context, key string, numShards int) (ShardID, error)
	ShardWithMeta(ctx context.Context, data []byte, numShards int, meta map[string]interface{}) ([]*Shard, error)
	Name() string
}

type HashSharder struct {
	hashFunc func([]byte) uint64
}

func NewHashSharder() *HashSharder {
	return &HashSharder{
		hashFunc: func(b []byte) uint64 {
			h := fnv.New64a()
			h.Write(b)
			return h.Sum64()
		},
	}
}

func (h *HashSharder) Shard(ctx context.Context, data []byte, numShards int) ([]*Shard, error) {
	if numShards <= 0 {
		numShards = 1
	}

	hash := h.hashFunc(data)
	shardID := ShardID(hash % uint64(numShards))

	shards := make([]*Shard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &Shard{
			ID:    ShardID(i),
			Data:  nil,
			Index: i,
			Total: numShards,
		}
	}

	shards[shardID].Data = data
	shards[shardID].Key = string(data)

	return shards, nil
}

func (h *HashSharder) ShardKey(ctx context.Context, key string, numShards int) (ShardID, error) {
	if numShards <= 0 {
		numShards = 1
	}

	hash := h.hashFunc([]byte(key))
	return ShardID(hash % uint64(numShards)), nil
}

func (h *HashSharder) ShardWithMeta(ctx context.Context, data []byte, numShards int, meta map[string]interface{}) ([]*Shard, error) {
	if numShards <= 0 {
		numShards = 1
	}

	hash := h.hashFunc(data)
	shardID := ShardID(hash % uint64(numShards))

	shards := make([]*Shard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &Shard{
			ID:    ShardID(i),
			Data:  nil,
			Index: i,
			Total: numShards,
			Meta:  make(map[string]interface{}),
		}
	}

	shards[shardID].Data = data
	shards[shardID].Key = string(data)
	shards[shardID].Meta = meta

	return shards, nil
}

func (h *HashSharder) Name() string {
	return "hash"
}

type RangeSharder struct {
	ranges []string
}

func NewRangeSharder(ranges []string) *RangeSharder {
	return &RangeSharder{
		ranges: ranges,
	}
}

func (r *RangeSharder) Shard(ctx context.Context, data []byte, numShards int) ([]*Shard, error) {
	if numShards <= 0 {
		numShards = 1
	}

	key := string(data)
	shardID := r.getShardIDForKey(key, numShards)

	shards := make([]*Shard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &Shard{
			ID:    ShardID(i),
			Data:  nil,
			Index: i,
			Total: numShards,
		}
	}

	shards[shardID].Data = data
	shards[shardID].Key = key

	return shards, nil
}

func (r *RangeSharder) ShardKey(ctx context.Context, key string, numShards int) (ShardID, error) {
	if numShards <= 0 {
		numShards = 1
	}
	return r.getShardIDForKey(key, numShards), nil
}

func (r *RangeSharder) ShardWithMeta(ctx context.Context, data []byte, numShards int, meta map[string]interface{}) ([]*Shard, error) {
	if numShards <= 0 {
		numShards = 1
	}

	key := string(data)
	shardID := r.getShardIDForKey(key, numShards)

	shards := make([]*Shard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &Shard{
			ID:    ShardID(i),
			Data:  nil,
			Index: i,
			Total: numShards,
			Meta:  make(map[string]interface{}),
		}
	}

	shards[shardID].Data = data
	shards[shardID].Key = key
	shards[shardID].Meta = meta

	return shards, nil
}

func (r *RangeSharder) getShardIDForKey(key string, numShards int) ShardID {
	if len(r.ranges) == 0 {
		h := fnv.New64a()
		h.Write([]byte(key))
		return ShardID(h.Sum64() % uint64(numShards))
	}

	for i, boundary := range r.ranges {
		if key < boundary {
			return ShardID(i % numShards)
		}
	}
	return ShardID((len(r.ranges)) % numShards)
}

func (r *RangeSharder) Name() string {
	return "range"
}

type RoundRobinSharder struct {
	currentShard int32
}

func NewRoundRobinSharder() *RoundRobinSharder {
	return &RoundRobinSharder{
		currentShard: 0,
	}
}

func (r *RoundRobinSharder) Shard(ctx context.Context, data []byte, numShards int) ([]*Shard, error) {
	if numShards <= 0 {
		numShards = 1
	}

	shardID := r.nextShard(numShards)

	shards := make([]*Shard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &Shard{
			ID:    ShardID(i),
			Data:  nil,
			Index: i,
			Total: numShards,
		}
	}

	shards[shardID].Data = data
	shards[shardID].Key = string(data)

	return shards, nil
}

func (r *RoundRobinSharder) ShardKey(ctx context.Context, key string, numShards int) (ShardID, error) {
	if numShards <= 0 {
		numShards = 1
	}
	return r.nextShard(numShards), nil
}

func (r *RoundRobinSharder) ShardWithMeta(ctx context.Context, data []byte, numShards int, meta map[string]interface{}) ([]*Shard, error) {
	if numShards <= 0 {
		numShards = 1
	}

	shardID := r.nextShard(numShards)

	shards := make([]*Shard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &Shard{
			ID:    ShardID(i),
			Data:  nil,
			Index: i,
			Total: numShards,
			Meta:  make(map[string]interface{}),
		}
	}

	shards[shardID].Data = data
	shards[shardID].Key = string(data)
	shards[shardID].Meta = meta

	return shards, nil
}

func (r *RoundRobinSharder) nextShard(numShards int) ShardID {
	current := r.currentShard
	r.currentShard = (r.currentShard + 1) % int32(numShards)
	return ShardID(current)
}

func (r *RoundRobinSharder) Name() string {
	return "round_robin"
}

type ShardingConfig struct {
	Type          string                 `json:"type"`
	NumShards     int                    `json:"num_shards"`
	HashField     string                 `json:"hash_field,omitempty"`
	RangeBoundaries []string             `json:"range_boundaries,omitempty"`
	Consistent    bool                   `json:"consistent,omitempty"`
	VirtualNodes  int                    `json:"virtual_nodes,omitempty"`
}

func NewSharderFromConfig(config ShardingConfig) Sharder {
	switch config.Type {
	case "hash":
		return NewHashSharder()
	case "range":
		return NewRangeSharder(config.RangeBoundaries)
	case "round_robin":
		return NewRoundRobinSharder()
	default:
		return NewHashSharder()
	}
}

type DataRecord struct {
	ID       string
	Key      string
	Data     []byte
	Schema   models.Schema
	Metadata map[string]interface{}
}

func NewDataRecord(id, key string, data []byte) *DataRecord {
	return &DataRecord{
		ID:       id,
		Key:      key,
		Data:     data,
		Metadata: make(map[string]interface{}),
	}
}

func (d *DataRecord) GetString(key string) string {
	if v, ok := d.Metadata[key].(string); ok {
		return v
	}
	return ""
}

func (d *DataRecord) SetMetadata(key string, value interface{}) {
	d.Metadata[key] = value
}
