package segmentio

import (
	"os"
	"testing"

	maasModel "github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/stretchr/testify/require"
)

// Every way a topic address can be rejected. These are the paths a misconfigured
// or partially filled MaaS response takes, so they need to fail with a message
// that says what was wrong rather than with a nil dereference later.
func Test_getAvailableData_RejectsUnusableProperties(t *testing.T) {
	caCert := readTestFile(t, "test/test-cert.pem")
	clientCert := readTestFile(t, "test/test-client-cert.pem")

	cases := []struct {
		name        string
		props       maasModel.TopicConnectionProperties
		wantMessage string
	}{
		{
			name:        "unknown protocol",
			props:       maasModel.TopicConnectionProperties{Protocol: "CARRIER_PIGEON"},
			wantMessage: "unsupported protocol",
		},
		{
			name:        "PLAINTEXT with a mechanism it cannot do",
			props:       maasModel.TopicConnectionProperties{Protocol: "PLAINTEXT", SaslMechanism: "GSSAPI"},
			wantMessage: "unsupported mechanism",
		},
		{
			name:        "SASL_PLAINTEXT accepts SCRAM only",
			props:       maasModel.TopicConnectionProperties{Protocol: "SASL_PLAINTEXT", SaslMechanism: "PLAIN"},
			wantMessage: "unsupported mechanism",
		},
		{
			name:        "SASL_SSL with a CA cert that is not PEM",
			props:       maasModel.TopicConnectionProperties{Protocol: "SASL_SSL", SaslMechanism: "PLAIN", CACert: "not a certificate"},
			wantMessage: "failed to append ca cert",
		},
		{
			name: "SASL_SSL with a mechanism it cannot do",
			props: maasModel.TopicConnectionProperties{
				Protocol: "SASL_SSL", SaslMechanism: "GSSAPI", CACert: caCert,
			},
			wantMessage: "unsupported SaslMechanism",
		},
		{
			name: "SASL_SSL with a client key that does not match the cert",
			props: maasModel.TopicConnectionProperties{
				Protocol: "SASL_SSL", SaslMechanism: "PLAIN", CACert: caCert,
				ClientCert: clientCert, ClientKey: "not a key",
			},
			wantMessage: "failed to create X509KeyPair",
		},
		{
			name:        "SSL with a CA cert that is not PEM",
			props:       maasModel.TopicConnectionProperties{Protocol: "SSL", CACert: "not a certificate"},
			wantMessage: "failed to append ca cert",
		},
		{
			name: "SSL with a client key that does not match the cert",
			props: maasModel.TopicConnectionProperties{
				Protocol: "SSL", CACert: caCert, ClientCert: clientCert, ClientKey: "not a key",
			},
			wantMessage: "failed to create X509KeyPair",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertions := require.New(t)
			props := tc.props
			servers, config, mechanism, err := getAvailableData(&props)
			assertions.Error(err)
			assertions.Contains(err.Error(), tc.wantMessage)
			assertions.Nil(servers)
			assertions.Nil(config)
			assertions.Nil(mechanism)
		})
	}
}

// An unusable topic address must not produce a half-built writer, reader config
// or client: every constructor has to hand the error back.
func Test_Constructors_PropagateTopicAddressErrors(t *testing.T) {
	unusable := maasModel.TopicAddress{
		TopicName:       "test",
		NumPartitions:   1,
		BoostrapServers: map[string][]string{"CARRIER_PIGEON": {"test.kafka:9092"}},
	}

	constructors := map[string]func() (any, error){
		"NewWriter": func() (any, error) { return NewWriter(unusable) },
		"NewReaderConfig": func() (any, error) {
			return NewReaderConfig(unusable, "group")
		},
		"NewClient": func() (any, error) { return NewClient(unusable) },
		"NewDialerAndServers": func() (any, error) {
			dialer, _, err := NewDialerAndServers(unusable)
			return dialer, err
		},
	}

	for name, build := range constructors {
		t.Run(name, func(t *testing.T) {
			assertions := require.New(t)
			result, err := build()
			assertions.Error(err)
			assertions.Nil(result)
		})
	}
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(content)
}
