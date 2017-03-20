# aster

紫菀【zǐwǎn シオン】

![](docs/images/harujion.png)

# How to use

## Consumer

Please refer to [Examples](examples/) and the comments in the codes.

Create a test topic

```
$ bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic test-topic --partitions 10 --replication-factor 1
```

Build and run worker

```
$ make example
$ bin/worker
```

Publish some messages:

```
$ bin/kafka-console-producer.sh --topic test-topic --broker-list localhost:9092
```

# License

MIT

