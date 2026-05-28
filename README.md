[![Go build](https://github.com/Netcracker/qubership-core-lib-go-maas-segmentio/actions/workflows/go-build.yml/badge.svg)](https://github.com/Netcracker/qubership-core-lib-go-maas-segmentio/actions/workflows/go-build.yml)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?metric=coverage&project=Netcracker_qubership-core-lib-go-maas-segmentio)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-maas-segmentio)
[![duplicated_lines_density](https://sonarcloud.io/api/project_badges/measure?metric=duplicated_lines_density&project=Netcracker_qubership-core-lib-go-maas-segmentio)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-maas-segmentio)
[![vulnerabilities](https://sonarcloud.io/api/project_badges/measure?metric=vulnerabilities&project=Netcracker_qubership-core-lib-go-maas-segmentio)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-maas-segmentio)
[![bugs](https://sonarcloud.io/api/project_badges/measure?metric=bugs&project=Netcracker_qubership-core-lib-go-maas-segmentio)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-maas-segmentio)
[![code_smells](https://sonarcloud.io/api/project_badges/measure?metric=code_smells&project=Netcracker_qubership-core-lib-go-maas-segmentio)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-maas-segmentio)

# segmentio

This lib provides methods to provision segmentio kafka-go structs by topic information received from MaaS.
The provided Writer and Reader will receive all required configuration regarding communication with Kafka server

<!-- TOC -->
* [segmentio](#segmentio)
    * [1. Writer. To create kafka-go Write struct based on response from MaaS, the following code can be used:](#1-writer-to-create-kafka-go-write-struct-based-on-response-from-maas-the-following-code-can-be-used)
    * [2. Reader. to create kafka-go Reader struct based on response from MaaS, the following code can be used:](#2-reader-to-create-kafka-go-reader-struct-based-on-response-from-maas-the-following-code-can-be-used)
      * [GetTopic example:](#gettopic-example)
      * [WatchTopicCreate example:](#watchtopiccreate-example)
      * [WatchTenantTopics example:](#watchtenanttopics-example)
    * [3. Customize underlying segmentio structs:](#3-customize-underlying-segmentio-structs)
<!-- TOC -->

### 1. Writer. To create kafka-go Write struct based on response from MaaS, the following code can be used:
  ~~~ go 
  import (
	"context"
	"fmt"
	"github.com/netcracker/qubership-core-lib-go/context-propagation/baseproviders"
	"github.com/netcracker/qubership-core-lib-go/context-propagation/ctxmanager"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	maas "github.com/netcracker/qubership-core-lib-go-maas-core"
	"github.com/netcracker/qubership-core-lib-go-maas-segmentio/v3"
	segmentioHelper "github.com/netcracker/qubership-core-lib-go-maas-segmentio/v3"
	kafkago "github.com/segmentio/kafka-go"
  )

func producer() {
	ctxmanager.Register(baseproviders.Get())
	ctx := context.Background()
	kafkaClient := maas.NewKafkaClient()
	topicAddress, _ := kafkaClient.GetOrCreateTopic(ctx, classifier.New("test"))
	writer, _ := segmentio.NewWriter(*topicAddress)
	ctxData, _ := ctxmanager.GetResponsePropagatableContextData(ctx)
	message := kafkago.Message{
		Key:     []byte("price"),
		Value:   []byte("10USD"),
		Headers: segmentioHelper.BuildHeaders(ctxData),
	}
	writer.WriteMessages(ctx, message)
}
  ~~~

### 2. Reader. to create kafka-go Reader struct based on response from MaaS, the following code can be used:
#### GetTopic example:
  ~~~ go
  import (
	"context"
	"fmt"
	"github.com/netcracker/qubership-core-lib-go/context-propagation/baseproviders"
	"github.com/netcracker/qubership-core-lib-go/context-propagation/ctxmanager"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	maas "github.com/netcracker/qubership-core-lib-go-maas-core"
	segmentioHelper "github.com/netcracker/qubership-core-lib-go-maas-segmentio/v3"
    "github.com/segmentio/kafka-go"
	"time"
)

func consumer() {
	ctxmanager.Register(baseproviders.Get())
	ctx := context.Background()
	kafkaClient := maas.NewKafkaClient()
	topicAddress, _ := kafkaClient.GetTopic(ctx, classifier.New("test"))
	readerConfig, _ := segmentioHelper.NewReaderConfig(*topicAddress, "prices-group-id")
	reader := kafka.NewReader(*readerConfig)
	defer reader.Close()
	for {
		msg, _ := reader.ReadMessage(ctx)
		localCtx := ctxmanager.InitContext(ctx, segmentioHelper.ExtractHeaders(msg.Headers))
		fmt.Printf("message=%v, ctx=%v", msg, localCtx)
	}
}
~~~

#### WatchTopicCreate example:
  ~~~ go
  import (
	"context"
	"fmt"
	"github.com/netcracker/qubership-core-lib-go/context-propagation/baseproviders"
	"github.com/netcracker/qubership-core-lib-go/context-propagation/ctxmanager"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	maas "github.com/netcracker/qubership-core-lib-go-maas-core"
	segmentioHelper "github.com/netcracker/qubership-core-lib-go-maas-segmentio/v3"
    "github.com/segmentio/kafka-go"
	"time"
)

func watchingConsumer() {
	ctxmanager.Register(baseproviders.Get())
	ctx := context.Background()
	kafkaClient := maas.NewKafkaClient()
	kafkaClient.WatchTopicCreate(ctx, classifier.New("test"), func(topicAddress model.TopicAddress) {
		readerConfig, _ := segmentioHelper.NewReaderConfig(topicAddress, "prices-group-id")
		reader := kafka.NewReader(*readerConfig)
		defer reader.Close()
		for {
			msg, _ := reader.ReadMessage(ctx)
			localCtx := ctxmanager.InitContext(ctx, segmentioHelper.ExtractHeaders(msg.Headers))
			fmt.Printf("message=%v, ctx=%v", msg, localCtx)
		}
	})
}
~~~

#### WatchTenantTopics example:
~~~ go
import (
	"context"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	kafkaCl "github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka"
	segmentioHelper "github.com/netcracker/qubership-core-lib-go-maas-segmentio/v3"
	"github.com/segmentio/kafka-go"
)

func watchingTenantTopics(ctx context.Context, readers []*kafka.Reader, kafkaClient kafkaCl.MaasClient) {
	kafkaClient.WatchTenantTopics(ctx, classifier.New("test"), func(topicAddresses []model.TopicAddress) {
		// close current readers
		for _, reader := range readers  {
			reader.Close()
		}
		// create new readers
		for _, topicAddress := range topicAddresses {
			tenantId := topicAddress.Classifier[classifier.TenantId]
			groupId := "tenant-prices-group-id-" + tenantId
			readerConfig, err := segmentioHelper.NewReaderConfig(topicAddress, groupId)
			if err != nil {
				panic(err.Error())
			}
			readers = append(readers, kafka.NewReader(*readerConfig))
		}
		// start consuming messages from readers
		// ...
	})
}
~~~

### 3. Customize underlying segmentio structs:
~~~ go 
  import (
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	segmentioHelper "github.com/netcracker/qubership-core-lib-go-maas-segmentio/v3"
  )

func writerWithOptions(topicAddress model.TopicAddress) {
    writer, err := segmentioHelper.NewWriter(topicAddress,
        segmentioHelper.WriterOptions{AlterTransport: func(transport *kafka.Transport) (*kafka.Transport, error) {
            transport.MetadataTTL = time.Minute
            transport.IdleTimeout = 2 * time.Minute
            transport.DialTimeout = 3 * time.Minute
            return transport, nil
        }})
}

func readerConfigWithOptions(topicAddress model.TopicAddress) {
	readerConfig, err := segmentioHelper.NewReaderConfig(topicAddress, "group-id",
		segmentioHelper.ReaderOptions{AlterDialer: func(dialer *kafka.Dialer) (*kafka.Dialer, error) {
			dialer.Timeout = time.Minute
			dialer.KeepAlive = 2 * time.Minute
			dialer.FallbackDelay = 3 * time.Minute
			return dialer, nil
		}})
}

func clientWithOptions(topicAddress model.TopicAddress) {
	client, err := segmentioHelper.NewClient(topicAddress,
		segmentioHelper.ClientOptions{AlterTransport: func(transport *kafka.Transport) (*kafka.Transport, error) {
			transport.MetadataTTL = time.Minute
			transport.IdleTimeout = 2 * time.Minute
			transport.DialTimeout = 3 * time.Minute
			return transport, nil
		}})
}
~~~

