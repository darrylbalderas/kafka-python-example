from confluent_kafka import Consumer, KafkaError, KafkaException
import logging

def print_assignment(consumer, partitions):
    print('Assignment:', partitions)

def main():
    broker = 'localhost:9092'
    group = "python-consumers"
    topic = 'inputTopic'
    conf = {'bootstrap.servers': broker, 'group.id': group, 
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest'}

    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)
    consumer = Consumer(conf, logger=logger)
    consumer.subscribe([topic], on_assign=print_assignment)
    try:
        while True:
            msg = consumer.poll(timeout=0.1)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('%s [%d] reached end at offset %d\n' %
                                        (msg.topic(), msg.partition(), msg.offset()))
                else:
                    raise KafkaException(msg.error())
            else:
                print('%s [%d] at offset %d with key %s with value: %s\n' %
                                    (msg.topic(), msg.partition(), msg.offset(),
                                    str(msg.key()), str(msg.value())))
    except KeyboardInterrupt:
        print('User is aborting program")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()