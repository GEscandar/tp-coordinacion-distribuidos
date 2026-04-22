import os
import logging
import signal

from common import middleware, message_protocol, fruit_item
from common.middleware.middleware import MessageMiddlewareDisconnectedError

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
SUM_EOF_QUEUE = f"{SUM_PREFIX}_eof_queue"
SUM_BCAST_EXCHANGE = f"{SUM_PREFIX}_bcast"
SUM_BCAST_QUEUE = f"{SUM_PREFIX}_bcast_{ID}"


class SumFilter:
    def __init__(self):
        self.multi_consumer = middleware.MultiQueueConsumer(MOM_HOST)
        self.multi_consumer.add_queue(INPUT_QUEUE, self.process_input_message)
        self.multi_consumer.add_queue(SUM_BCAST_QUEUE, self.process_bcast_message)
        self.bcast_exchange = middleware.FanoutExchange(
            MOM_HOST, SUM_BCAST_EXCHANGE, SUM_BCAST_QUEUE
        )
        self.data_output_queues = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{AGGREGATION_PREFIX}_{i}"
            )
            self.data_output_queues.append(data_output_queue)
        self.eof_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, SUM_EOF_QUEUE
        )
        self.amount_by_fruit_by_client = {}
        self.clients_with_eof = set()
        self._closed = False

    def _flush_client(self, client_id):
        if client_id in self.clients_with_eof:
            return
        self.clients_with_eof.add(client_id)

        client_fruits = self.amount_by_fruit_by_client.pop(client_id, {})
        target_aggregation = client_id % AGGREGATION_AMOUNT

        if client_fruits:
            logging.info(f"Sum_{ID}: Sending aggregated data for client {client_id} to aggregation {target_aggregation}")
            for final_fruit_item in client_fruits.values():
                serialized = message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
                self.data_output_queues[target_aggregation].send(serialized)

        logging.info(f"Sum_{ID}: Sending EOF for client {client_id} to aggregation {target_aggregation}")
        self.data_output_queues[target_aggregation].send(
            message_protocol.internal.serialize([client_id])
        )

        logging.info(f"Sum_{ID}: Sending EOF notification for client {client_id} to join")
        self.eof_output_queue.send(
            message_protocol.internal.serialize([client_id])
        )

    def _process_data(self, client_id, fruit, amount):
        if client_id not in self.amount_by_fruit_by_client:
            self.amount_by_fruit_by_client[client_id] = {}
        client_fruits = self.amount_by_fruit_by_client[client_id]
        client_fruits[fruit] = client_fruits.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Sum_{ID}: Received EOF for client {client_id}")
        self._flush_client(client_id)

        bcast_msg = message_protocol.internal.serialize(["bcast", client_id, ID])
        self.bcast_exchange.send(bcast_msg)

    def _process_bcast(self, client_id, source_id):
        if source_id == ID:
            return
        logging.info(f"Sum_{ID}: Received broadcast from sum_{source_id} for client {client_id}")
        self._flush_client(client_id)

    def process_input_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                self._process_data(*fields)
            else:
                self._process_eof(fields[0])
            ack()
        except Exception as e:
            logging.error(f"Error processing input message: {e}")
            nack()

    def process_bcast_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            self._process_bcast(fields[1], fields[2])
            ack()
        except Exception as e:
            logging.error(f"Error processing bcast message: {e}")
            nack()

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self._closed = True
        self.multi_consumer.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.multi_consumer.start_consuming()

    def close(self):
        if not self._closed:
            self.multi_consumer.close()
        for q in self.data_output_queues:
            q.close()
        self.eof_output_queue.close()
        self.bcast_exchange.close()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    try:
        sum_filter.start()
    except MessageMiddlewareDisconnectedError:
        logging.info("Middleware disconnected")
    finally:
        sum_filter.close()
    return 0


if __name__ == "__main__":
    main()
