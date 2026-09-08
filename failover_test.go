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
	// broker is lost, which is what makes a leader election possible.
	failoverBrokers  = 3
	failoverReplicas = 2
	// kafkaImageVersion is the Confluent image the cluster runs.
	kafkaImageVersion = "7.4.0"
	// writeTimeout bounds one produce call, so a lost leader surfaces as an
	// error rather than a hang.
	writeTimeout = 20 * time.Second
	// consumeTimeout bounds reading back what was produced.
	consumeTimeout = 30 * time.Second
	// warmUpKey is written to prove the topic is writable before a test measures
	// anything. It is skipped when reading back.
	warmUpKey = "warm-up"
	// recoveryAllowance is how long a writer is given to find a new leader. It
	// has to outlast the metadata cache of kafka-go, which keeps addressing the
	// broker it last knew as the leader until the cache expires.
	recoveryAllowance = 90 * time.Second
)

// startFailoverCluster brings up a cluster and a topic for one test.
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
// in-sync replica list is not enough to conclude it will: the list is maintained
// on a lag timeout, so a replica counts as in sync before its fetcher has caught
// up, and until then the leader cannot satisfy acks=all and answers
// REQUEST_TIMED_OUT.
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

// writerFor builds the writer under test. The batch timeout is cut down because
// these tests write one message at a time, and the default would spend a second
// waiting for a batch that is never coming.
func writerFor(t *testing.T, address maasModel.TopicAddress) *kafka.Writer {
	t.Helper()
	writer, err := NewWriter(address)
	require.NoError(t, err)
	writer.BatchTimeout = 100 * time.Millisecond
	writer.ErrorLogger = kafka.LoggerFunc(t.Logf)
	t.Cleanup(func() { writer.Close() })
	return writer
}

// produce writes keys count..count+n and reports which of them the broker
// acknowledged. What is not acknowledged is not expected back. A failure carries
// the key and how long the write took, which is what tells a rejected write
// apart from one that waited out its deadline.
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

// awaitAccepted retries one write until the broker takes it, and reports how
// long that took. A single write with a short deadline right after a leader loss
// only proves the loss: the writer is still addressing the broker that is gone.
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

// consumeAll reads the topic from its first offset until it goes quiet, and
// returns how many times each key arrived.
func consumeAll(t *testing.T, address maasModel.TopicAddress, groupId string) map[string]int {
	t.Helper()
	config, err := NewReaderConfig(address, groupId)
	require.NoError(t, err)
	config.StartOffset = kafka.FirstOffset
	reader := kafka.NewReader(*config)
	defer reader.Close()

	seen := map[string]int{}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), consumeTimeout)
		message, err := reader.FetchMessage(ctx)
		cancel()
		if err != nil {
			// only a timeout means the topic is drained; anything else means the
			// read never worked, and an empty result would be read as data loss
			require.ErrorIs(t, err, context.DeadlineExceeded, "reading the topic back failed")
			return seen
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
// the new leader on its own; what it acknowledged before the loss must still be
// readable afterwards.
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
	require.NoError(t, cluster.stopBroker(ctx, leader))
	require.NoError(t, cluster.awaitLeaders(ctx, topic, 1))

	recovery := awaitAccepted(t, writer, "10")
	t.Logf("the writer found the new leader in %s", recovery)
	afterLoss, failures := produce(ctx, writer, 11, 20)
	assert.Empty(t, failures, "writes kept failing after the writer had recovered")

	acknowledged := append(beforeLoss, "10")
	acknowledged = append(acknowledged, afterLoss...)
	seen := consumeAll(t, address, "leader-loss-reader")
	assert.Empty(t, lost(acknowledged, seen),
		"acks=all must not lose an acknowledged message when the leader goes")
	// a write rejected on a timeout may still have been appended, so the retries
	// behind the recovery are a legitimate source of duplicates
	t.Logf("duplicates: %v", duplicated(seen))
}

// What an acks setting is worth during a leader loss, measured rather than
// argued. acks=0 is the kafka-go zero value and was what this library shipped
// before it started setting RequiredAcks.
func TestFailover_AcksDecideWhatSurvivesALeaderLoss(t *testing.T) {
	for _, acks := range []kafka.RequiredAcks{kafka.RequireNone, kafka.RequireOne, kafka.RequireAll} {
		t.Run(acks.String(), func(t *testing.T) {
			topic := "failover-acks-" + acks.String()
			cluster, address := startFailoverCluster(t, topic, 1)
			ctx := context.Background()

			writer := writerFor(t, address)
			writer.RequiredAcks = acks

			acknowledged, failures := produce(ctx, writer, 0, 20)

			leaders, err := cluster.partitionLeaders(ctx, topic)
			require.NoError(t, err)
			require.NoError(t, cluster.stopBroker(ctx, leaders[0]))
			require.NoError(t, cluster.awaitLeaders(ctx, topic, 1))

			seen := consumeAll(t, address, "acks-reader-"+acks.String())
			missing := lost(acknowledged, seen)
			t.Logf("acks=%s: %d writes reported success, %d of them are not in the topic, %d reported an error",
				acks, len(acknowledged), len(missing), len(failures))
			assert.NotEmpty(t, seen, "the topic must still be readable after the leader changed")
		})
	}
}

// NewWriter must not leave the kafka-go zero value: acks=0 never reads a broker
// response, so a write that never reached the log still reports success.
func TestFailover_NewWriterDoesNotAcknowledgeNothing(t *testing.T) {
	writer, err := NewWriter(topicAddressOf("any", 1, []string{"127.0.0.1:9092"}))
	require.NoError(t, err)
	defer writer.Close()

	assert.Equal(t, kafka.RequireOne, writer.RequiredAcks)
}

// The seed list is fixed when the writer is built and never re-read from MaaS.
// A writer told about one broker cannot use the rest of a healthy cluster once
// that broker is gone.
func TestFailover_WriterDoesNotLookBeyondItsSeedList(t *testing.T) {
	const topic = "failover-seed-list"
	cluster, _ := startFailoverCluster(t, topic, 1)
	ctx := context.Background()

	leaders, err := cluster.partitionLeaders(ctx, topic)
	require.NoError(t, err)
	leader := leaders[0]

	writer := writerFor(t, topicAddressOf(topic, 1, []string{cluster.brokerAddress(leader)}))

	_, failures := produce(ctx, writer, 0, 5)
	require.Empty(t, failures)

	require.NoError(t, cluster.stopBroker(ctx, leader))
	require.NoError(t, cluster.awaitLeaders(ctx, topic, 1))

	_, failures = produce(ctx, writer, 5, 10)
	assert.NotEmpty(t, failures,
		"the writer reached a broker outside its seed list, so the seed list is no longer fixed")

	require.NoError(t, cluster.startBroker(ctx, leader))
	require.NoError(t, cluster.awaitLeaders(ctx, topic, 1))
	acknowledged, failures := produce(ctx, writer, 10, 15)
	assert.NotEmpty(t, acknowledged, "the writer did not recover once its seed broker came back: %v", failures)
}

// Every broker is replaced in turn, which is the node sweep from the problem
// statement. With one replica left at all times the topic stays writable.
func TestFailover_RollingRestartKeepsTheTopicWritable(t *testing.T) {
	const topic = "failover-rolling-restart"
	cluster, address := startFailoverCluster(t, topic, 1)
	ctx := context.Background()

	writer := writerFor(t, address)
	writer.RequiredAcks = kafka.RequireAll

	var acknowledged []string
	for broker := 0; broker < failoverBrokers; broker++ {
		require.NoError(t, cluster.stopBroker(ctx, broker), fmt.Sprintf("stopping broker %d", broker))
		require.NoError(t, cluster.awaitLeaders(ctx, topic, 1))

		first := strconv.Itoa(broker * 10)
		t.Logf("broker %d down, the writer recovered in %s", broker, awaitAccepted(t, writer, first))
		written, failures := produce(ctx, writer, broker*10+1, broker*10+5)
		assert.Empty(t, failures, "writes kept failing while broker %d was down", broker)
		acknowledged = append(append(acknowledged, first), written...)

		require.NoError(t, cluster.startBroker(ctx, broker))
		require.NoError(t, cluster.awaitLeaders(ctx, topic, 1))
	}

	seen := consumeAll(t, address, "rolling-restart-reader")
	assert.Empty(t, lost(acknowledged, seen), "a message acknowledged during the sweep was lost")
}
