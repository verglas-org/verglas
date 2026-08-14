import os
import urllib.request

import pika


connection = pika.BlockingConnection(
    pika.URLParameters(os.environ["RABBITMQ_URL"])
)
channel = connection.channel()
queue = os.environ.get("RABBITMQ_QUEUE", "market-quotes")
channel.queue_declare(queue=queue, durable=True)
channel.basic_qos(prefetch_count=1)


def forward(ch, method, properties, body):
    if properties.content_type != "application/cloudevents+json":
        raise ValueError("message must contain a structured CloudEvent")
    request = urllib.request.Request(
        os.environ["VERGLAS_EVENTS_URL"],
        data=body,
        method="POST",
        headers={"content-type": "application/cloudevents+json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status != 202:
            raise RuntimeError(f"Verglas returned HTTP {response.status}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue=queue, on_message_callback=forward)
channel.start_consuming()
