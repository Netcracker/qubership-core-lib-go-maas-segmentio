package segmentio

import (
	"testing"

	maasModel "github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

// The broker address list is resolved once when the Writer is built; later
// changes to the source topic address must not affect an already-built Writer.
func Test_NewWriter_BrokerListIsFrozenAtConstruction(t *testing.T) {
	assertions := require.New(t)
	topic := maasModel.TopicAddress{
		TopicName:       "test",
		NumPartitions:   1,
		BoostrapServers: map[string][]string{"PLAINTEXT": {"broker1:9092"}},
	}

	writer, err := NewWriter(topic)
	assertions.NoError(err)

	topic.BoostrapServers["PLAINTEXT"][0] = "broker2:9092"

	assertions.Contains(writer.Addr.String(), "broker1")
	assertions.NotContains(writer.Addr.String(), "broker2")
}

// Writes must wait for at least one broker acknowledgement; fire-and-forget
// acks let a message get silently dropped during a partition leader change.
func Test_NewWriter_RequiredAcksIsExplicit(t *testing.T) {
	assertions := require.New(t)
	writer, err := NewWriter(testTopicAddress())
	assertions.NoError(err)
	assertions.NotEqual(kafka.RequireNone, writer.RequiredAcks,
		"acks=0 means the broker response is never read, so a leader change drops messages silently")
	assertions.Equal(kafka.RequireOne, writer.RequiredAcks,
		"default is RequireOne: catches leader loss without paying slowest-ISR latency "+
			"or failing whenever ISR drops below min.insync.replicas")
}

// Callers that need different durability must be able to override it without
// reaching into the returned writer by hand.
func Test_NewWriter_AlterWriterCanOverrideRequiredAcks(t *testing.T) {
	assertions := require.New(t)
	writer, err := NewWriter(testTopicAddress(), WriterOptions{
		AlterWriter: func(w *kafka.Writer) (*kafka.Writer, error) {
			w.RequiredAcks = kafka.RequireAll
			return w, nil
		},
	})
	assertions.NoError(err)
	assertions.Equal(kafka.RequireAll, writer.RequiredAcks)
}

func testTopicAddress() maasModel.TopicAddress {
	return maasModel.TopicAddress{
		TopicName:       "test",
		NumPartitions:   1,
		BoostrapServers: map[string][]string{"PLAINTEXT": {"test.kafka:9092"}},
	}
}
