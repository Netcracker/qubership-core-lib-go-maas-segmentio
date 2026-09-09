package segmentio

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	maasModel "github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// failoverBrokers and failoverReplicas leave one replica standing when a
	// broker is lost.
	failoverBrokers  = 3
	failoverReplicas = 2
	// kafkaImageVersion is the Confluent image the cluster runs.
	kafkaImageVersion = "7.4.0"
	// writeTimeout bounds one produce call.
	writeTimeout = 20 * time.Second
	// consumeTimeout bounds one read while draining a partition.
	consumeTimeout = 30 * time.Second
	// metadataTTL is how long a writer may keep addressing the broker it last
	// knew as the leader.
	metadataTTL = time.Second
	// recoveryAllowance is how long a writer is given to find a new leader.
	recoveryAllowance = 15 * time.Second
	// warmUpKey is written before a test measures anything, and skipped on read.
	warmUpKey = "warm-up"
	// offsetsTopic holds the group offsets. It has one partition, so the broker
	// leading it coordinates every group.
	offsetsTopic = "__consumer_offsets"
	// readerRecoveryAllowance is how long a reader is given to reach the new
	// coordinator.
	readerRecoveryAllowance = 60 * time.Second
	// fetchStep bounds one read or commit attempt during a recovery.
	fetchStep = 5 * time.Second
)

// startFailoverCluster brings up a cluster and a writable topic for one test.
func startFailoverCluster(t *testing.T, topic string, partitions int) (*kafkaTestCluster, maasModel.TopicAddress) {
	t.Helper()
	useDockerHostFromEnv()

	ctx := context.Background()
	cluster, err := newKafkaCluster(ctx, kafkaImageVersion, failoverBrokers, failoverReplicas)
	require.NoError(t, err)
	t.Cleanup(func() { cluster.stop(context.Background()) })

	require.NoError(t, cluster.createTopic(ctx, topic, partitions, failoverReplicas))
	address := topicAddressOf(topic, partitions, cluster.brokers())
	awaitWritable(t, address)
	return cluster, address
}

// awaitWritable produces until the topic accepts a write with acks=all. A full
// in-sync replica list does not mean it will: the list is maintained on a lag
// timeout, and a replica counts as in sync before its fetcher has caught up.
func awaitWritable(t *testing.T, address maasModel.TopicAddress) {
	t.Helper()
	writer := writerFor(t, address)
	writer.RequiredAcks = kafka.RequireAll

	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(warmUpKey), Value: []byte(warmUpKey)})
		cancel()
		if err == nil {
			return
		}
	}
	t.Fatal("the topic never accepted a write with acks=all")
}

func topicAddressOf(topic string, partitions int, servers []string) maasModel.TopicAddress {
	return maasModel.TopicAddress{
		Classifier:      classifier.New("failover"),
		TopicName:       topic,
		NumPartitions:   partitions,
		BoostrapServers: map[string][]string{"PLAINTEXT": servers},
	}
}

// writerFor builds the writer under test, with the batch timeout and the
// metadata cache cut down to test scale.
func writerFor(t *testing.T, address maasModel.TopicAddress) *kafka.Writer {
	t.Helper()
	writer, err := NewWriter(address, WriterOptions{
		AlterTransport: func(transport *kafka.Transport) (*kafka.Transport, error) {
			transport.MetadataTTL = metadataTTL
			return transport, nil
		},
	})
	require.NoError(t, err)
	writer.BatchTimeout = 100 * time.Millisecond
	writer.ErrorLogger = kafka.LoggerFunc(t.Logf)
	t.Cleanup(func() { writer.Close() })
	return writer
}

// readerFor builds the reader under test, reading the topic from its start.
func readerFor(t *testing.T, address maasModel.TopicAddress, groupId string) *kafka.Reader {
	t.Helper()
	config, err := NewReaderConfig(address, groupId)
	require.NoError(t, err)
	config.StartOffset = kafka.FirstOffset
	reader := kafka.NewReader(*config)
	t.Cleanup(func() { reader.Close() })
	return reader
}

// fetchWithRecovery returns the next message, retrying while the group has no
// coordinator and skipping the warm-up record.
func fetchWithRecovery(t *testing.T, reader *kafka.Reader, allowance time.Duration) kafka.Message {
	t.Helper()
	deadline := time.Now().Add(allowance)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), fetchStep)
		message, err := reader.FetchMessage(ctx)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		if string(message.Key) == warmUpKey {
			continue
		}
		return message
	}
	t.Fatalf("no message arrived within %s, last error: %v", allowance, lastErr)
	return kafka.Message{}
}

// commitWithRecovery commits one message, retrying while the group has no
// coordinator.
func commitWithRecovery(t *testing.T, reader *kafka.Reader, message kafka.Message, allowance time.Duration) {
	t.Helper()
	start := time.Now()
	var lastErr error
	for time.Since(start) < allowance {
		ctx, cancel := context.WithTimeout(context.Background(), fetchStep)
		err := reader.CommitMessages(ctx, message)
		cancel()
		if err == nil {
			return
		}
		lastErr = err
	}
	t.Fatalf("the offset was not committed within %s, last error: %v", allowance, lastErr)
}

// produce writes keys from..to and reports which of them were acknowledged. A
// failure carries the key and how long the write took.
func produce(ctx context.Context, writer *kafka.Writer, from, to int) (acknowledged []string, failures []error) {
	for i := from; i < to; i++ {
		key := strconv.Itoa(i)
		writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
		start := time.Now()
		err := writer.WriteMessages(writeCtx, kafka.Message{Key: []byte(key), Value: []byte(key)})
		elapsed := time.Since(start)
		cancel()
		if err != nil {
			failures = append(failures, fmt.Errorf("key %s after %s: %w", key, elapsed.Round(time.Millisecond), err))
			continue
		}
		acknowledged = append(acknowledged, key)
	}
	return acknowledged, failures
}

// awaitAccepted retries one write until it is accepted, and reports how long
// that took.
func awaitAccepted(t *testing.T, writer *kafka.Writer, key string) time.Duration {
	t.Helper()
	start := time.Now()
	for time.Since(start) < recoveryAllowance {
		ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
		err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: []byte(key)})
		cancel()
		if err == nil {
			return time.Since(start)
		}
	}
	t.Fatalf("the writer never had a write accepted within %s", recoveryAllowance)
	return 0
}

// consumeAll reads every partition from its first offset and reports how many
// times each key arrived. It reads by partition, so it needs no coordinator.
func consumeAll(t *testing.T, address maasModel.TopicAddress) map[string]int {
	t.Helper()
	dialer, servers, err := NewDialerAndServers(address)
	require.NoError(t, err)

	seen := map[string]int{}
	for partition := 0; partition < address.NumPartitions; partition++ {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Dialer:    dialer,
			Brokers:   servers,
			Topic:     address.TopicName,
			Partition: partition,
		})
		require.NoError(t, reader.SetOffset(kafka.FirstOffset))
		drain(t, reader, seen)
		reader.Close()
	}
	return seen
}

// drain reads one partition until it goes quiet. Only a timeout ends it: any
// other error would leave an empty result that looks like data loss.
func drain(t *testing.T, reader *kafka.Reader, seen map[string]int) {
	t.Helper()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), consumeTimeout)
		message, err := reader.FetchMessage(ctx)
		cancel()
		if err != nil {
			require.ErrorIs(t, err, context.DeadlineExceeded, "reading the topic back failed")
			return
		}
		if key := string(message.Key); key != warmUpKey {
			seen[key]++
		}
	}
}

// lost reports the acknowledged keys that never came back.
func lost(acknowledged []string, seen map[string]int) []string {
	var missing []string
	for _, key := range acknowledged {
		if seen[key] == 0 {
			missing = append(missing, key)
		}
	}
	return missing
}

// duplicated reports the keys that came back more than once.
func duplicated(seen map[string]int) []string {
	var repeated []string
	for key, count := range seen {
		if count > 1 {
			repeated = append(repeated, key)
		}
	}
	return repeated
}

// A partition leader is lost while a producer is writing. The writer has to find
// the new leader on its own, and what it acknowledged must still be readable.
func TestFailover_ProducerSurvivesPartitionLeaderLoss(t *testing.T) {
	const topic = "failover-leader-loss"
	cluster, address := startFailoverCluster(t, topic, 1)
	ctx := context.Background()

	writer := writerFor(t, address)
	writer.RequiredAcks = kafka.RequireAll

	t.Log("before producing:\n" + cluster.describe(ctx, topic))
	beforeLoss, failures := produce(ctx, writer, 0, 10)
	require.Empty(t, failures, "the cluster was healthy, nothing should have failed")

	leaders, err := cluster.partitionLeaders(ctx, topic)
	require.NoError(t, err)
	leader := leaders[0]
	t.Logf("stopping broker %d, which leads partition 0", leader)
	require.NoError(t, cluster.stopBroker(ctx, leader))
	require.NoError(t, cluster.awaitLeaders(ctx, topic, 1))

	recovery := awaitAccepted(t, writer, "10")
	t.Logf("the writer found the new leader in %s", recovery)
	afterLoss, failures := produce(ctx, writer, 11, 20)
	assert.Empty(t, failures, "writes kept failing after the writer had recovered")

	acknowledged := append(beforeLoss, "10")
	acknowledged = append(acknowledged, afterLoss...)
	seen := consumeAll(t, address)
	assert.Empty(t, lost(acknowledged, seen),
		"acks=all must not lose an acknowledged message when the leader goes")
	// a write rejected on a timeout may still have been appended
	t.Logf("duplicates: %v", duplicated(seen))

	require.NoError(t, cluster.startBroker(ctx, leader))
	require.NoError(t, cluster.awaitLeaders(ctx, topic, 1))
}

// The group coordinator is lost while a reader is reading. Everything produced
// has to arrive and committing has to work again, without rebuilding the reader.
func TestFailover_ReaderSurvivesCoordinatorLoss(t *testing.T) {
	const topic = "failover-coordinator-loss"
	cluster, address := startFailoverCluster(t, topic, 1)
	ctx := context.Background()

	writer := writerFor(t, address)
	writer.RequiredAcks = kafka.RequireAll
	beforeLoss, failures := produce(ctx, writer, 0, 5)
	require.Empty(t, failures, "the cluster was healthy, nothing should have failed")

	reader := readerFor(t, address, "coordinator-loss-reader")
	seen := map[string]bool{}
	for range beforeLoss {
		message := fetchWithRecovery(t, reader, readerRecoveryAllowance)
		require.NoError(t, reader.CommitMessages(ctx, message))
		seen[string(message.Key)] = true
	}
	require.Len(t, seen, len(beforeLoss), "the messages produced before the loss must all arrive")

	// the offsets topic is created by the first commit, so the coordinator is
	// only known at this point
	leaders, err := cluster.partitionLeaders(ctx, offsetsTopic)
	require.NoError(t, err)
	coordinator := leaders[0]
	t.Logf("stopping broker %d, which coordinates the group", coordinator)
	require.NoError(t, cluster.stopBroker(ctx, coordinator))
	require.NoError(t, cluster.awaitLeaders(ctx, topic, 1))

	afterLoss, failures := produce(ctx, writer, 5, 10)
	assert.Empty(t, failures, "producing has to survive the loss as well")

	// delivery is at least once: an uncommitted offset is read again, so the set
	// of keys is the measure here rather than the count of messages
	expected := len(beforeLoss) + len(afterLoss)
	start := time.Now()
	delivered := 0
	for len(seen) < expected && time.Since(start) < readerRecoveryAllowance {
		message := fetchWithRecovery(t, reader, readerRecoveryAllowance)
		commitWithRecovery(t, reader, message, readerRecoveryAllowance)
		seen[string(message.Key)] = true
		delivered++
	}
	t.Logf("the reader delivered %d messages, %d of them new, in %s",
		delivered, len(seen)-len(beforeLoss), time.Since(start).Round(time.Millisecond))
	assert.Len(t, seen, expected, "losing the coordinator must not lose a message")

	require.NoError(t, cluster.startBroker(ctx, coordinator))
	require.NoError(t, cluster.awaitLeaders(ctx, topic, 1))
}
