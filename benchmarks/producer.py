# -*- coding: utf-8 -*-

from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')


if __name__ == '__main__':
    s='0123456789abcdef'
    i=0
    while i<13:
        i+=1
        s = s +s

    message_count = 30000

    for _ in range(message_count):
        producer.send('test-6', s)

    producer.flush()
