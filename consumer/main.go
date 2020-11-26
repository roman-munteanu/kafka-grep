package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/riferrei/srclient"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gopkg.in/linkedin/goavro.v2"
)

func main() {

	bootstrapServers := flag.String("bootstrapServers", "", "Kafka server")
	schemaRegistryURL := flag.String("schemaRegistryUrl", "", "Schema Registry URL")
	groupID := flag.String("groupId", "", "Consumer group")
	topic := flag.String("topic", "", "Topic")
	shemaFile := flag.String("shemaFile", "", "Schema file")
	grep := flag.String("grep", "", "String to filter by")
	flag.Parse()

	// validation
	if len(*bootstrapServers) == 0 || len(*topic) == 0 || len(*topic) == 0 {
		fmt.Println("Please provide required parameters: bootstrapServers, groupId, topic, schemaRegistryUrl or shemaFile")
		return
	}

	if len(*schemaRegistryURL) == 0 && len(*shemaFile) == 0 {
		fmt.Println("Either schemaRegistryUrl or shemaFile should be provided")
		return
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": *bootstrapServers,
		"group.id":          *groupID,
		"auto.offset.reset": "earliest",
	})
	logError(err)

	consumer.SubscribeTopics([]string{*topic}, nil)

	var schRegClient *srclient.SchemaRegistryClient
	var codec *goavro.Codec

	if len(*schemaRegistryURL) > 0 {

		schRegClient = srclient.CreateSchemaRegistryClient(*schemaRegistryURL)

	} else if len(*shemaFile) > 0 {
		schemaBs, err := ioutil.ReadFile(*shemaFile)
		logError(err)

		codec, err = goavro.NewCodec(string(schemaBs))
		if err != nil {
			fmt.Printf("CODEC: %v\n", err)
		}
	}

	for {
		msg, err := consumer.ReadMessage(-1)

		if err == nil {
			// filter messages
			if len(*grep) > 0 {
				content := string(msg.Value)
				if !strings.Contains(content, *grep) {
					continue
				}
			}

			if len(*schemaRegistryURL) > 0 {
				schemaID := binary.BigEndian.Uint32(msg.Value[1:5])
				fmt.Printf("schemaID: %d\n", schemaID)

				schema, err := schRegClient.GetSchema(int(schemaID))
				if err != nil {
					panic(fmt.Sprintf("Cannot get the shema by id '%d' %s\n", schemaID, err))
				}

				codec, err = goavro.NewCodec(schema.Schema())
				if err != nil {
					fmt.Printf("CODEC from SCHEMA: %v\n", err)
				}
			}

			native, _, err := codec.NativeFromBinary(msg.Value[5:])
			if err != nil {
				panic(fmt.Sprintf("NATIVE: %s\n", err))
			}

			value, _ := codec.TextualFromNative(nil, native)
			if err != nil {
				panic(fmt.Sprintf("TEXTUAL: %s\n", err))
			}

			fmt.Println(string(value))

		} else {
			fmt.Printf("Consumer error: %v for message: %v\n", err, msg)
		}
	}

	consumer.Close()
}

func logError(err error) {
	if err != nil {
		panic(err)
	}
}
