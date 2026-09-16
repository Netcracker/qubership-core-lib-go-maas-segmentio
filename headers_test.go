package segmentio

import (
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func Test_BuildHeaders(t *testing.T) {
	assertions := require.New(t)

	headers := BuildHeaders(map[string]string{"x-request-id": "42", "tenant": "acme"})

	assertions.Len(headers, 2)
	// the map has no order, so compare through the round trip
	assertions.Equal(map[string]interface{}{"x-request-id": "42", "tenant": "acme"},
		ExtractHeaders(headers))
}

func Test_BuildHeaders_Empty(t *testing.T) {
	assertions := require.New(t)

	assertions.Empty(BuildHeaders(map[string]string{}))
	assertions.Empty(BuildHeaders(nil))
}

func Test_ExtractHeaders(t *testing.T) {
	assertions := require.New(t)

	extracted := ExtractHeaders([]kafkago.Header{
		{Key: "x-request-id", Value: []byte("42")},
		{Key: "empty", Value: nil},
	})

	// values come back as strings, not as the raw byte slices
	assertions.Equal(map[string]interface{}{"x-request-id": "42", "empty": ""}, extracted)
}

func Test_ExtractHeaders_Empty(t *testing.T) {
	assertions := require.New(t)

	assertions.Empty(ExtractHeaders(nil))
	assertions.NotNil(ExtractHeaders(nil), "an empty result must still be a usable map")
}

// A duplicated key keeps the last value, which is what a map round trip gives.
func Test_ExtractHeaders_DuplicateKey(t *testing.T) {
	assertions := require.New(t)

	extracted := ExtractHeaders([]kafkago.Header{
		{Key: "retry", Value: []byte("first")},
		{Key: "retry", Value: []byte("second")},
	})

	assertions.Equal(map[string]interface{}{"retry": "second"}, extracted)
}
