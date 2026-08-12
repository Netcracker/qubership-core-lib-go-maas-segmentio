package segmentio

import (
	"errors"
	"testing"

	maasModel "github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

// Writes must wait for at least one broker acknowledgement. With acks=0 the
// writer never reads a broker response, so a partition leader change drops
// in-flight messages while WriteMessages still reports success.
func Test_NewWriter_RequiredAcksIsExplicit(t *testing.T) {
	assertions := require.New(t)
	writer, err := NewWriter(testTopicAddress())
	assertions.NoError(err)
	assertions.Equal(kafka.RequireOne, writer.RequiredAcks,
		"the kafka-go zero value is RequireNone, which reports success without ever hearing from a broker")
}

// The returned writer is mutable, so durability is a caller decision and needs
// no dedicated option.
func Test_NewWriter_RequiredAcksCanBeOverriddenOnTheResult(t *testing.T) {
	assertions := require.New(t)
	writer, err := NewWriter(testTopicAddress())
	assertions.NoError(err)

	writer.RequiredAcks = kafka.RequireAll
	assertions.Equal(kafka.RequireAll, writer.RequiredAcks)
}

// A hook that fails, or that hands back nil, must surface as an error naming the
// hook rather than as a nil field discovered later at the first write.
func Test_Alter_HooksReportFailureAndNil(t *testing.T) {
	hookErr := errors.New("boom")

	cases := []struct {
		name        string
		build       func() error
		wantMessage string
	}{
		{
			name: "AlterTransport error, writer",
			build: func() error {
				_, err := NewWriter(testTopicAddress(), WriterOptions{
					AlterTransport: func(*kafka.Transport) (*kafka.Transport, error) { return nil, hookErr },
				})
				return err
			},
			wantMessage: "AlterTransport failed",
		},
		{
			name: "AlterTransport nil, writer",
			build: func() error {
				_, err := NewWriter(testTopicAddress(), WriterOptions{
					AlterTransport: func(*kafka.Transport) (*kafka.Transport, error) { return nil, nil },
				})
				return err
			},
			wantMessage: "AlterTransport returned nil",
		},
		{
			name: "AlterTransport error, client",
			build: func() error {
				_, err := NewClient(testTopicAddress(), ClientOptions{
					AlterTransport: func(*kafka.Transport) (*kafka.Transport, error) { return nil, hookErr },
				})
				return err
			},
			wantMessage: "AlterTransport failed",
		},
		{
			name: "AlterTransport nil, client",
			build: func() error {
				_, err := NewClient(testTopicAddress(), ClientOptions{
					AlterTransport: func(*kafka.Transport) (*kafka.Transport, error) { return nil, nil },
				})
				return err
			},
			wantMessage: "AlterTransport returned nil",
		},
		{
			name: "AlterDialer error, reader config",
			build: func() error {
				_, err := NewReaderConfig(testTopicAddress(), "group", ReaderOptions{
					AlterDialer: func(*kafka.Dialer) (*kafka.Dialer, error) { return nil, hookErr },
				})
				return err
			},
			wantMessage: "AlterDialer failed",
		},
		{
			name: "AlterDialer nil, reader config",
			build: func() error {
				_, err := NewReaderConfig(testTopicAddress(), "group", ReaderOptions{
					AlterDialer: func(*kafka.Dialer) (*kafka.Dialer, error) { return nil, nil },
				})
				return err
			},
			wantMessage: "AlterDialer returned nil",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertions := require.New(t)
			err := tc.build()
			assertions.Error(err)
			assertions.Contains(err.Error(), tc.wantMessage)
		})
	}
}

// A hook that fails must not have its cause swallowed on the way out.
func Test_Alter_HookErrorIsWrapped(t *testing.T) {
	assertions := require.New(t)
	hookErr := errors.New("boom")

	_, err := NewWriter(testTopicAddress(), WriterOptions{
		AlterTransport: func(*kafka.Transport) (*kafka.Transport, error) { return nil, hookErr },
	})
	assertions.ErrorIs(err, hookErr)
}

// A hook that is set applies; one that is left nil leaves the value untouched.
func Test_Alter_HookAppliesAndNilHookIsSkipped(t *testing.T) {
	assertions := require.New(t)

	replacement := &kafka.Transport{}
	writer, err := NewWriter(testTopicAddress(), WriterOptions{
		AlterTransport: func(*kafka.Transport) (*kafka.Transport, error) { return replacement, nil },
	})
	assertions.NoError(err)
	assertions.Same(replacement, writer.Transport)

	untouched, err := NewWriter(testTopicAddress(), WriterOptions{})
	assertions.NoError(err)
	assertions.NotNil(untouched.Transport)
}

// PLAINTEXT without a SASL mechanism: the simplest address that builds, and the
// only place the no-credentials branch of getAvailableData is exercised.
func testTopicAddress() maasModel.TopicAddress {
	return maasModel.TopicAddress{
		TopicName:       "test",
		NumPartitions:   1,
		BoostrapServers: map[string][]string{"PLAINTEXT": {"test.kafka:9092"}},
	}
}
