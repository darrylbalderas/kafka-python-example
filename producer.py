from confluent_kafka import Producer
import logging

def delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s\n' % err)
    else:
        print('%% Message delivered to %s [%d] @ %s\n' % (msg.topic(), msg.partition(), msg.offset()))

def main():
    messages = ['The litte lambe', 'The king has arrived']
    logger = logging.getLogger('producer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)
    conf = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(conf, logger=logger)
    topic = 'inputTopic'
    for message in messages:
        try:
            producer.produce(topic, message, "key", callback=delivery_callback)
        except:
            print('Local producer queue is full (%d messages awaiting delivery): try again\n' % len(producer))
        producer.poll(0)
    print("Waiting for deliveries")
    producer.flush()

if __name__ == '__main__':
    main()