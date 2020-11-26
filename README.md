kafka-grep
-----

# Purpose

Provide a custom Kafka consumer with the filter option

# Setup

```
sudo apt-get install build-essential

go get -u gopkg.in/confluentinc/confluent-kafka-go.v1/kafka

go get gopkg.in/linkedin/goavro.v2
```

# Build

```
env GOOS=linux go build main.go
```

# Usage

using a separate schema file:
```
./main -bootstrapServers localhost:9092 -groupId my-group-id -topic my-topic -shemaFile /path/to/schema.json -grep FilterByThisString
```