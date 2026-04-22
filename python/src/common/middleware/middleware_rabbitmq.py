import pika
import logging
import threading
import time

from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)


def _create_connection(host):
    retries = 50
    for i in range(retries):
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
        except pika.exceptions.AMQPConnectionError:
            if i == retries - 1:
                raise MessageMiddlewareDisconnectedError()
            time.sleep(1)
        except pika.exceptions.AMQPError:
            if i == retries - 1:
                raise MessageMiddlewareDisconnectedError()
            time.sleep(1)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._host = host
        self._queue_name = queue_name
        self._connection = _create_connection(host)
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=queue_name, durable=True)
        self._consumer_tag = None
        self._consuming = False
        self._lock = threading.Lock()

    def start_consuming(self, on_message_callback):
        self._consuming = True

        def _internal_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        self._channel.basic_qos(prefetch_count=1)
        self._consumer_tag = self._channel.basic_consume(
            queue=self._queue_name,
            on_message_callback=_internal_callback,
        )
        try:
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            pass

    def stop_consuming(self):
        if self._consuming:
            self._consuming = False
            try:
                self._connection.add_callback_threadsafe(
                    lambda: self._channel.stop_consuming()
                )
            except Exception:
                try:
                    self._channel.stop_consuming()
                except Exception:
                    pass

    def send(self, message):
        with self._lock:
            try:
                self._channel.basic_publish(
                    exchange="",
                    routing_key=self._queue_name,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2),
                )
            except pika.exceptions.AMQPConnectionError:
                raise MessageMiddlewareDisconnectedError()
            except Exception as e:
                raise MessageMiddlewareMessageError(str(e))

    def close(self):
        try:
            self.stop_consuming()
            self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self._host = host
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        self._connection = _create_connection(host)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(
            exchange=exchange_name, exchange_type="direct", durable=True
        )
        for routing_key in routing_keys:
            queue_name = routing_key
            self._channel.queue_declare(queue=queue_name, durable=True)
            self._channel.queue_bind(
                exchange=exchange_name,
                queue=queue_name,
                routing_key=routing_key,
            )
        self._consumer_tag = None
        self._consuming = False
        self._lock = threading.Lock()

    def start_consuming(self, on_message_callback):
        self._consuming = True
        if len(self._routing_keys) != 1:
            raise MessageMiddlewareMessageError(
                "Exchange consumer must have exactly one routing key"
            )
        queue_name = self._routing_keys[0]

        def _internal_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        self._channel.basic_qos(prefetch_count=1)
        self._consumer_tag = self._channel.basic_consume(
            queue=queue_name,
            on_message_callback=_internal_callback,
        )
        try:
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            pass

    def stop_consuming(self):
        if self._consuming:
            self._consuming = False
            try:
                self._connection.add_callback_threadsafe(
                    lambda: self._channel.stop_consuming()
                )
            except Exception:
                try:
                    self._channel.stop_consuming()
                except Exception:
                    pass

    def send(self, message):
        with self._lock:
            try:
                for routing_key in self._routing_keys:
                    self._channel.basic_publish(
                        exchange=self._exchange_name,
                        routing_key=routing_key,
                        body=message,
                        properties=pika.BasicProperties(delivery_mode=2),
                    )
            except pika.exceptions.AMQPConnectionError:
                raise MessageMiddlewareDisconnectedError()
            except Exception as e:
                raise MessageMiddlewareMessageError(str(e))

    def close(self):
        try:
            self.stop_consuming()
            self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))


class FanoutExchange:
    def __init__(self, host, exchange_name, queue_name):
        self._host = host
        self._exchange_name = exchange_name
        self._queue_name = queue_name
        self._connection = _create_connection(host)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(
            exchange=exchange_name, exchange_type="fanout", durable=True
        )
        self._channel.queue_declare(queue=queue_name, durable=True)
        self._channel.queue_bind(
            exchange=exchange_name, queue=queue_name
        )

    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange=self._exchange_name,
                routing_key="",
                body=message,
                properties=pika.BasicProperties(delivery_mode=2),
            )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def close(self):
        try:
            self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))


class MultiQueueConsumer:
    def __init__(self, host):
        self._host = host
        self._connection = _create_connection(host)
        self._channel = self._connection.channel()
        self._queues = {}
        self._consuming = False

    def add_queue(self, queue_name, callback):
        self._channel.queue_declare(queue=queue_name, durable=True)
        self._queues[queue_name] = callback

    def start_consuming(self):
        self._consuming = True
        self._channel.basic_qos(prefetch_count=1)
        for queue_name, callback in self._queues.items():
            def _internal_callback(ch, method, properties, body, cb=callback):
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)

                cb(body, ack, nack)

            self._channel.basic_consume(
                queue=queue_name,
                on_message_callback=_internal_callback,
            )
        try:
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            pass

    def stop_consuming(self):
        if self._consuming:
            self._consuming = False
            try:
                self._connection.add_callback_threadsafe(
                    lambda: self._channel.stop_consuming()
                )
            except Exception:
                try:
                    self._channel.stop_consuming()
                except Exception:
                    pass

    def close(self):
        try:
            self.stop_consuming()
            self._connection.close()
        except Exception:
            pass
