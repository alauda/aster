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


## Develop new consumer

Please refer [Examples](examples/) directory.

Mainly you need 3 steps:

* Implement `ProcessMessage(message *kafka.Message) error` method, this method will consume the kafka message.
* Implement `init()` function:
    ```
    func init() {
        Register(WORKER_NAME, Init)
    }
    ```
* Implement `Init() (*ConsumerWorker, error)` function, this function will create a new Consumer.

* Optional `StartDaemon` and `StopDaemon` if you want run some daemon work but not action on new message sent.

## Install Mac OS

```
$ brew install pkg-config

$ brew install librdkafka

```

# License

MIT

