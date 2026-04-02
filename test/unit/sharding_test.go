package unit

import (
	"context"
	"testing"

	"datapipe/internal/sharding"
)

func TestHashSharder(t *testing.T) {
	ctx := context.Background()
	sharder := sharding.NewHashSharder()

	t.Run("basic sharding", func(t *testing.T) {
		data := []byte("test-key-1")
		numShards := 5

		shards, err := sharder.Shard(ctx, data, numShards)
		if err != nil {
			t.Fatalf("failed to shard: %v", err)
		}

		if len(shards) != numShards {
			t.Errorf("expected %d shards, got %d", numShards, len(shards))
		}

		filledShard := -1
		for i, shard := range shards {
			if shard.Data != nil {
				if filledShard != -1 {
					t.Error("more than one shard has data")
				}
				filledShard = i
			}
		}

		if filledShard == -1 {
			t.Error("no shard has data")
		}
	})

	t.Run("consistent sharding for same key", func(t *testing.T) {
		data := []byte("consistent-key")
		numShards := 10

		shards1, _ := sharder.Shard(ctx, data, numShards)
		shards2, _ := sharder.Shard(ctx, data, numShards)

		shardID1 := -1
		shardID2 := -1

		for i, shard := range shards1 {
			if shard.Data != nil {
				shardID1 = i
				break
			}
		}

		for i, shard := range shards2 {
			if shard.Data != nil {
				shardID2 = i
				break
			}
		}

		if shardID1 != shardID2 {
			t.Errorf("inconsistent sharding: got %d and %d", shardID1, shardID2)
		}
	})

	t.Run("shard key", func(t *testing.T) {
		key := "test-key"
		numShards := 5

		shardID1, err := sharder.ShardKey(ctx, key, numShards)
		if err != nil {
			t.Fatalf("failed to shard key: %v", err)
		}

		shardID2, err := sharder.ShardKey(ctx, key, numShards)
		if err != nil {
			t.Fatalf("failed to shard key: %v", err)
		}

		if shardID1 != shardID2 {
			t.Errorf("inconsistent key sharding: got %d and %d", shardID1, shardID2)
		}
	})

	t.Run("shard with meta", func(t *testing.T) {
		data := []byte("test-key-with-meta")
		numShards := 3
		meta := map[string]interface{}{
			"timestamp": 1234567890,
			"source":    "test",
		}

		shards, err := sharder.ShardWithMeta(ctx, data, numShards, meta)
		if err != nil {
			t.Fatalf("failed to shard with meta: %v", err)
		}

		found := false
		for _, shard := range shards {
			if shard.Data != nil {
				if shard.Meta == nil {
					t.Error("expected meta to be set on filled shard")
				}
				if shard.Meta["timestamp"] != float64(1234567890) {
					t.Errorf("expected timestamp=1234567890, got %v", shard.Meta["timestamp"])
				}
				found = true
				break
			}
		}

		if !found {
			t.Error("no shard has data")
		}
	})

	t.Run("zero shards", func(t *testing.T) {
		data := []byte("test-key")
		shards, err := sharder.Shard(ctx, data, 0)

		if err != nil {
			t.Fatalf("should handle zero shards: %v", err)
		}

		if len(shards) != 1 {
			t.Errorf("expected 1 shard for zero input, got %d", len(shards))
		}
	})

	t.Run("sharder name", func(t *testing.T) {
		if sharder.Name() != "hash" {
			t.Errorf("expected name 'hash', got '%s'", sharder.Name())
		}
	})
}

func TestRoundRobinSharder(t *testing.T) {
	ctx := context.Background()
	sharder := sharding.NewRoundRobinSharder()

	t.Run("round robin distribution", func(t *testing.T) {
		numShards := 3
		keys := []string{"key1", "key2", "key3", "key4", "key5"}

		distribution := make(map[int]int)
		for _, key := range keys {
			shardID, err := sharder.ShardKey(ctx, key, numShards)
			if err != nil {
				t.Fatalf("failed to shard key %s: %v", key, err)
			}
			distribution[int(shardID)]++
		}

		if len(distribution) > numShards {
			t.Error("distribution has more shards than expected")
		}
	})

	t.Run("sequential distribution", func(t *testing.T) {
		data := []byte("data")
		numShards := 3

		shardIDs := make([]int, 6)
		for i := 0; i < 6; i++ {
			shards, err := sharder.Shard(ctx, data, numShards)
			if err != nil {
				t.Fatalf("failed to shard: %v", err)
			}

			for j, shard := range shards {
				if shard.Data != nil {
					shardIDs[i] = j
					break
				}
			}
		}

		expected := []int{0, 1, 2, 0, 1, 2}
		for i, expectedID := range expected {
			if shardIDs[i] != expectedID {
				t.Errorf("at index %d: expected %d, got %d", i, expectedID, shardIDs[i])
			}
		}
	})

	t.Run("shard with meta", func(t *testing.T) {
		data := []byte("test-data")
		numShards := 2
		meta := map[string]interface{}{
			"custom": "value",
		}

		shards, err := sharder.ShardWithMeta(ctx, data, numShards, meta)
		if err != nil {
			t.Fatalf("failed to shard with meta: %v", err)
		}

		for _, shard := range shards {
			if shard.Data != nil {
				if shard.Meta["custom"] != "value" {
					t.Errorf("expected custom='value', got %v", shard.Meta["custom"])
				}
			}
		}
	})

	t.Run("sharder name", func(t *testing.T) {
		if sharder.Name() != "round_robin" {
			t.Errorf("expected name 'round_robin', got '%s'", sharder.Name())
		}
	})
}

func TestShardingDistribution(t *testing.T) {
	ctx := context.Background()

	t.Run("hash distribution uniformity", func(t *testing.T) {
		sharder := sharding.NewHashSharder()
		numShards := 10
		numKeys := 1000

		distribution := make(map[int]int)
		for i := 0; i < numKeys; i++ {
			key := []byte(string(rune('a' + i%26)))
			shards, err := sharder.Shard(ctx, key, numShards)
			if err != nil {
				t.Fatalf("failed to shard: %v", err)
			}

			for j, shard := range shards {
				if shard.Data != nil {
					distribution[j]++
					break
				}
			}
		}

		if len(distribution) == 0 {
			t.Error("no distribution recorded")
		}

		avgPerShard := numKeys / numShards
		for shardID, count := range distribution {
			if count == 0 {
				t.Errorf("shard %d has no keys (uneven distribution)", shardID)
			}
			if float64(count) < float64(avgPerShard)*0.5 {
				t.Errorf("shard %d has only %d keys, expected around %d (uneven distribution)", shardID, count, avgPerShard)
			}
		}
	})

	t.Run("range sharding", func(t *testing.T) {
		ranges := []string{"m", "p", "v"}
		sharder := sharding.NewRangeSharder(ranges)
		numShards := 3

		testCases := []struct {
			key      string
			expected int
		}{
			{"apple", 0},
			{"banana", 1},
			{"pear", 2},
			{"watermelon", 2},
			{"zucchini", 2},
		}

		for _, tc := range testCases {
			shardID, err := sharder.ShardKey(ctx, tc.key, numShards)
			if err != nil {
				t.Fatalf("failed to shard key %s: %v", tc.key, err)
			}

			if int(shardID) != tc.expected {
				t.Errorf("for key %s: expected shard %d, got %d", tc.key, tc.expected, shardID)
			}
		}
	})

	t.Run("sharding config", func(t *testing.T) {
		hashConfig := sharding.ShardingConfig{
			Type:      "hash",
			NumShards: 5,
		}

		hashSharder := sharding.NewSharderFromConfig(hashConfig)
		if hashSharder.Name() != "hash" {
			t.Errorf("expected hash sharder, got %s", hashSharder.Name())
		}

		rrConfig := sharding.ShardingConfig{
			Type:      "round_robin",
			NumShards: 3,
		}

		rrSharder := sharding.NewSharderFromConfig(rrConfig)
		if rrSharder.Name() != "round_robin" {
			t.Errorf("expected round_robin sharder, got %s", rrSharder.Name())
		}

		rangeConfig := sharding.ShardingConfig{
			Type:            "range",
			NumShards:       4,
			RangeBoundaries: []string{"g", "n", "t"},
		}

		rangeSharder := sharding.NewSharderFromConfig(rangeConfig)
		if rangeSharder.Name() != "range" {
			t.Errorf("expected range sharder, got %s", rangeSharder.Name())
		}
	})
}

func TestDataRecord(t *testing.T) {
	t.Run("create data record", func(t *testing.T) {
		record := sharding.NewDataRecord("id1", "key1", []byte("data"))

		if record.ID != "id1" {
			t.Errorf("expected ID 'id1', got '%s'", record.ID)
		}
		if record.Key != "key1" {
			t.Errorf("expected Key 'key1', got '%s'", record.Key)
		}
		if string(record.Data) != "data" {
			t.Errorf("expected Data 'data', got '%s'", string(record.Data))
		}
		if record.Metadata == nil {
			t.Error("expected non-nil Metadata")
		}
	})

	t.Run("set metadata", func(t *testing.T) {
		record := sharding.NewDataRecord("id1", "key1", []byte("data"))

		record.SetMetadata("field1", "value1")
		record.SetMetadata("count", 42)

		if record.Metadata["field1"] != "value1" {
			t.Errorf("expected field1='value1', got '%v'", record.Metadata["field1"])
		}
		if record.Metadata["count"] != 42 {
			t.Errorf("expected count=42, got '%v'", record.Metadata["count"])
		}
	})

	t.Run("get string metadata", func(t *testing.T) {
		record := sharding.NewDataRecord("id1", "key1", []byte("data"))
		record.SetMetadata("stringField", "stringValue")

		result := record.GetString("stringField")
		if result != "stringValue" {
			t.Errorf("expected 'stringValue', got '%s'", result)
		}
	})

	t.Run("get string non-existent", func(t *testing.T) {
		record := sharding.NewDataRecord("id1", "key1", []byte("data"))

		result := record.GetString("nonExistent")
		if result != "" {
			t.Errorf("expected empty string for non-existent key, got '%s'", result)
		}
	})
}

func TestShardMetadata(t *testing.T) {
	ctx := context.Background()
	sharder := sharding.NewHashSharder()

	t.Run("shard metadata preservation", func(t *testing.T) {
		data := []byte("key-with-meta")
		numShards := 3
		meta := map[string]interface{}{
			"source":   "test",
			"priority": 1,
		}

		shards, err := sharder.ShardWithMeta(ctx, data, numShards, meta)
		if err != nil {
			t.Fatalf("failed to shard with meta: %v", err)
		}

		for _, shard := range shards {
			if shard.Data != nil {
				if shard.Meta["source"] != "test" {
					t.Errorf("expected source='test', got %v", shard.Meta["source"])
				}
				if shard.Meta["priority"] != float64(1) {
					t.Errorf("expected priority=1, got %v", shard.Meta["priority"])
				}
			}
		}
	})

	t.Run("shard index and total", func(t *testing.T) {
		data := []byte("test-data")
		numShards := 5

		shards, err := sharder.Shard(ctx, data, numShards)
		if err != nil {
			t.Fatalf("failed to shard: %v", err)
		}

		for i, shard := range shards {
			if shard.Index != i {
				t.Errorf("expected index %d, got %d", i, shard.Index)
			}
			if shard.Total != numShards {
				t.Errorf("expected total %d, got %d", numShards, shard.Total)
			}
			if shard.ID != sharding.ShardID(i) {
				t.Errorf("expected ID %d, got %d", i, shard.ID)
			}
		}
	})
}
