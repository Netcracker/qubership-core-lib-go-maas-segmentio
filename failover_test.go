package segmentio

import (
	"testing"

	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	maasModel "github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func topicAddressOf(topic string, partitions int, servers []string) maasModel.TopicAddress {
	return maasModel.TopicAddress{
		Classifier:      classifier.New("failover"),
		TopicName:       topic,
		NumPartitions:   partitions,
		BoostrapServers: map[string][]string{"PLAINTEXT": servers},
	}
}

// NewWriter must not leave the kafka-go zero value: acks=0 never reads a broker
// response, so a write that never reached the log still reports success, and a
// leader lost right after accepting it takes it away unnoticed.
func TestFailover_NewWriterDoesNotAcknowledgeNothing(t *testing.T) {
	writer, err := NewWriter(topicAddressOf("any", 1, []string{"127.0.0.1:9092"}))
	require.NoError(t, err)
	defer writer.Close()

	assert.Equal(t, kafka.RequireOne, writer.RequiredAcks)
}
