from setup import topic
def pass_to_kafka(dic):
    with topic.get_sync_producer(linger_ms=0) as p:
        while True:
            message = dic
            p.produce(str(message))
            try:
                p.stop()
            except ProducerStoppedException:
                print("Message sent")