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
	// warmUpKey is written to prove the topic is writable before a test measures
	// anything.
	warmUpKey = "warm-up"
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

// produce writes keys from..to and reports which of them the broker
// acknowledged. A failure carries the key and how long the write took.
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

	_, failures = produce(ctx, writer, 5, 10)
	assert.NotEmpty(t, failures,
		"the writer reached a broker outside its seed list, so the seed list is no longer fixed")

	require.NoError(t, cluster.startBroker(ctx, leader))
	require.NoError(t, cluster.awaitLeaders(ctx, topic, 1))
	acknowledged, failures := produce(ctx, writer, 10, 15)
	assert.NotEmpty(t, acknowledged, "the writer did not recover once its seed broker came back: %v", failures)
}
