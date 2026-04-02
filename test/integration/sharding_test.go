package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"datapipe/internal/sharding"
)

func TestShardingParallelExecution(t *testing.T) {
	t.Run("parallel sharding with multiple workers", func(t *testing.T) {
		sharder := sharding.NewHashSharder()
		numShards := 4
		numKeys := 100
		numWorkers := 4

		shardCounts := make([]int32, numShards)
		var wg sync.WaitGroup

		ctx := context.Background()

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for i := 0; i < numKeys/numWorkers; i++ {
					key := []byte(string(rune('a' + (workerID*numKeys/numWorkers+i)%26)))
					shards, err := sharder.Shard(ctx, key, numShards)
					if err != nil {
						t.Errorf("worker %d: failed to shard: %v", workerID, err)
						continue
					}

					for _, shard := range shards {
						if shard.Data != nil {
							atomic.AddInt32(&shardCounts[shard.Index], 1)
						}
					}
				}
			}(w)
		}

		wg.Wait()

		totalDistributed := int32(0)
		for i := 0; i < numShards; i++ {
			totalDistributed += shardCounts[i]
			t.Logf("Shard %d: %d items", i, shardCounts[i])
		}

		if totalDistributed != int32(numKeys) {
			t.Errorf("expected %d total distributed items, got %d", numKeys, totalDistributed)
		}

		for i := 0; i < numShards; i++ {
			if shardCounts[i] == 0 {
				t.Logf("Shard %d has no items - distribution may be uneven", i)
			}
		}
	})

	t.Run("sharding consistency across multiple calls", func(t *testing.T) {
		sharder := sharding.NewHashSharder()
		ctx := context.Background()
		key := []byte("consistent-test-key")
		numShards := 10

		firstShardID := -1
		for i := 0; i < 100; i++ {
			shards, err := sharder.Shard(ctx, key, numShards)
			if err != nil {
				t.Fatalf("failed to shard: %v", err)
			}

			for j, shard := range shards {
				if shard.Data != nil {
					if firstShardID == -1 {
						firstShardID = j
					} else if j != firstShardID {
						t.Errorf("inconsistent sharding: first was %d, now is %d", firstShardID, j)
					}
					break
				}
			}
		}

		if firstShardID == -1 {
			t.Error("no shard was assigned the data")
		} else {
			t.Logf("Consistent shard assignment: %d", firstShardID)
		}
	})

	t.Run("round robin load balancing", func(t *testing.T) {
		sharder := sharding.NewRoundRobinSharder()
		ctx := context.Background()
		numShards := 3
		numOps := 30

		shardCounts := make([]int32, numShards)
		var mu sync.Mutex

		for i := 0; i < numOps; i++ {
			go func(opID int) {
				data := []byte(string(rune('a' + opID)))
				shards, err := sharder.Shard(ctx, data, numShards)
				if err != nil {
					t.Errorf("failed to shard: %v", err)
					return
				}

				mu.Lock()
				defer mu.Unlock()
				for _, shard := range shards {
					if shard.Data != nil {
						shardCounts[shard.Index]++
					}
				}
			}(i)
		}

		t.Logf("Round robin distribution after %d operations: %v", numOps, shardCounts)

		total := int32(0)
		for i := 0; i < numShards; i++ {
			total += shardCounts[i]
		}

		if total != int32(numOps) {
			t.Errorf("expected %d total operations, got %d", numOps, total)
		}
	})
}

func TestShardingWithMetadata(t *testing.T) {
	t.Run("metadata preservation across sharding", func(t *testing.T) {
		sharder := sharding.NewHashSharder()
		ctx := context.Background()

		testData := []byte("test-data-with-meta")
		numShards := 5
		meta := map[string]interface{}{
			"source":       "integration-test",
			"timestamp":    1234567890,
			"processing":   "parallel",
			"shard_config": map[string]int{"replicas": 3},
		}

		shards, err := sharder.ShardWithMeta(ctx, testData, numShards, meta)
		if err != nil {
			t.Fatalf("failed to shard with meta: %v", err)
		}

		for _, shard := range shards {
			if shard.Data != nil {
				if shard.Meta == nil {
					t.Fatal("expected metadata on filled shard")
				}

				if shard.Meta["source"] != "integration-test" {
					t.Errorf("expected source='integration-test', got %v", shard.Meta["source"])
				}

				if shard.Meta["timestamp"] != float64(1234567890) {
					t.Errorf("expected timestamp=1234567890, got %v", shard.Meta["timestamp"])
				}

				nested, ok := shard.Meta["shard_config"].(map[string]interface{})
				if !ok {
					t.Fatal("expected nested map in metadata")
				}
				if nested["replicas"] != float64(3) {
					t.Errorf("expected replicas=3, got %v", nested["replicas"])
				}

				t.Logf("Shard %d metadata preserved: %v", shard.Index, shard.Meta)
				break
			}
		}
	})
}

func TestShardingAggregation(t *testing.T) {
	t.Run("result aggregation from multiple shards", func(t *testing.T) {
		type ShardResult struct {
			ShardID    int
			ItemCount  int
			SampleData []string
		}

		results := make([]*ShardResult, 4)

		for i := 0; i < len(results); i++ {
			results[i] = &ShardResult{
				ShardID:    i,
				ItemCount:  (i + 1) * 10,
				SampleData: []string{"item1", "item2"},
			}
		}

		totalItems := 0
		for _, r := range results {
			totalItems += r.ItemCount
		}

		expectedTotal := 10 + 20 + 30 + 40
		if totalItems != expectedTotal {
			t.Errorf("expected total %d, got %d", expectedTotal, totalItems)
		}

		t.Logf("Aggregated results: total items = %d", totalItems)

		allData := []string{}
		for _, r := range results {
			allData = append(allData, r.SampleData...)
		}

		if len(allData) != 8 {
			t.Errorf("expected 8 total sample items, got %d", len(allData))
		}
	})
}

func TestShardingPerformance(t *testing.T) {
	t.Run("high throughput sharding", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping performance test in short mode")
		}

		sharder := sharding.NewHashSharder()
		ctx := context.Background()
		numShards := 10
		numOps := 10000

		done := make(chan struct{})
		var processed int64

		go func() {
			for i := 0; i < numOps; i++ {
				key := []byte(string(rune('a' + i%26)))
				_, err := sharder.Shard(ctx, key, numShards)
				if err != nil {
					t.Errorf("sharding failed: %v", err)
				}
				atomic.AddInt64(&processed, 1)
			}
			close(done)
		}()

		select {
		case <-done:
			t.Logf("Processed %d sharding operations", processed)
		}
	})
}

func TestShardingEdgeCases(t *testing.T) {
	t.Run("single shard", func(t *testing.T) {
		sharder := sharding.NewHashSharder()
		ctx := context.Background()

		data := []byte("test-data")
		shards, err := sharder.Shard(ctx, data, 1)
		if err != nil {
			t.Fatalf("failed to shard: %v", err)
		}

		if len(shards) != 1 {
			t.Errorf("expected 1 shard, got %d", len(shards))
		}

		if shards[0].Data == nil {
			t.Error("expected data in single shard")
		}
	})

	t.Run("zero shards", func(t *testing.T) {
		sharder := sharding.NewHashSharder()
		ctx := context.Background()

		data := []byte("test-data")
		shards, err := sharder.Shard(ctx, data, 0)
		if err != nil {
			t.Fatalf("failed to shard with 0 shards: %v", err)
		}

		if len(shards) != 1 {
			t.Errorf("expected 1 shard for 0 input, got %d", len(shards))
		}
	})

	t.Run("negative shards", func(t *testing.T) {
		sharder := sharding.NewHashSharder()
		ctx := context.Background()

		data := []byte("test-data")
		shards, err := sharder.Shard(ctx, data, -5)
		if err != nil {
			t.Fatalf("failed to shard with negative shards: %v", err)
		}

		if len(shards) != 1 {
			t.Errorf("expected 1 shard for negative input, got %d", len(shards))
		}
	})

	t.Run("empty data", func(t *testing.T) {
		sharder := sharding.NewHashSharder()
		ctx := context.Background()

		data := []byte{}
		shards, err := sharder.Shard(ctx, data, 5)
		if err != nil {
			t.Fatalf("failed to shard empty data: %v", err)
		}

		if len(shards) != 5 {
			t.Errorf("expected 5 shards, got %d", len(shards))
		}

		filledCount := 0
		for _, shard := range shards {
			if shard.Data != nil {
				filledCount++
			}
		}

		if filledCount != 1 {
			t.Errorf("expected 1 filled shard, got %d", filledCount)
		}
	})
}

func TestShardingKeyDistribution(t *testing.T) {
	t.Run("alphabetical key distribution", func(t *testing.T) {
		sharder := sharding.NewHashSharder()
		ctx := context.Background()
		numShards := 4

		keyDistribution := make(map[int][]string)

		for i := 0; i < 26; i++ {
			key := string(rune('a' + i))
			shardID, err := sharder.ShardKey(ctx, key, numShards)
			if err != nil {
				t.Fatalf("failed to shard key %s: %v", key, err)
			}

			keyDistribution[int(shardID)] = append(keyDistribution[int(shardID)], key)
		}

		t.Log("Key distribution across shards:")
		for shardID, keys := range keyDistribution {
			t.Logf("  Shard %d: %v", shardID, keys)
		}

		nonEmptyShards := 0
		for i := 0; i < numShards; i++ {
			if len(keyDistribution[i]) > 0 {
				nonEmptyShards++
			}
		}

		if nonEmptyShards < 2 {
			t.Errorf("expected keys distributed across multiple shards, only %d shard(s) used", nonEmptyShards)
		}
	})
}

func TestShardInterface(t *testing.T) {
	t.Run("sharder interface implementation", func(t *testing.T) {
		var _ sharding.Sharder = (*sharding.HashSharder)(nil)
		var _ sharding.Sharder = (*sharding.RoundRobinSharder)(nil)
		var _ sharding.Sharder = (*sharding.RangeSharder)(nil)

		t.Log("All sharders implement Sharder interface")
	})

	t.Run("shard ID type", func(t *testing.T) {
		var id sharding.ShardID = 5

		if int(id) != 5 {
			t.Errorf("expected shard ID 5, got %d", int(id))
		}

		t.Logf("ShardID type works correctly: %d", id)
	})
}

func TestDataRecordSharding(t *testing.T) {
	t.Run("data record with sharding", func(t *testing.T) {
		sharder := sharding.NewHashSharder()
		ctx := context.Background()

		record := sharding.NewDataRecord("record-1", "key-a", []byte("test data"))

		shards, err := sharder.Shard(ctx, []byte(record.Key), 5)
		if err != nil {
			t.Fatalf("failed to shard record: %v", err)
		}

		for _, shard := range shards {
			if shard.Data != nil {
				if shard.Key != record.Key {
					t.Errorf("expected key '%s', got '%s'", record.Key, shard.Key)
				}
				t.Logf("Record %s sharded to shard %d", record.ID, shard.Index)
				break
			}
		}
	})
}
