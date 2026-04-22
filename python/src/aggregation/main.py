import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{AGGREGATION_PREFIX}_{ID}"
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_item_by_client = {}
        self.eof_count_by_client = {}
        self._closed = False

    def _process_data(self, client_id, fruit, amount):
        if client_id not in self.fruit_item_by_client:
            self.fruit_item_by_client[client_id] = {}
        client_fruits = self.fruit_item_by_client[client_id]
        if fruit in client_fruits:
            client_fruits[fruit] = client_fruits[fruit] + fruit_item.FruitItem(
                fruit, amount
            )
        else:
            client_fruits[fruit] = fruit_item.FruitItem(fruit, amount)

    def _process_eof(self, client_id):
        self.eof_count_by_client[client_id] = self.eof_count_by_client.get(client_id, 0) + 1
        if self.eof_count_by_client[client_id] < SUM_AMOUNT:
            logging.info(f"Received EOF {self.eof_count_by_client[client_id]}/{SUM_AMOUNT} for client {client_id}")
            return
        logging.info(f"Received all EOFs for client {client_id}")
        client_fruits = self.fruit_item_by_client.pop(client_id, {})

        fruit_items = list(client_fruits.values())
        fruit_items.sort(reverse=True)
        top_size = min(TOP_SIZE, len(fruit_items))
        fruit_top = fruit_items[:top_size]

        fruit_top_list = list(
            map(
                lambda fi: (fi.fruit, fi.amount),
                fruit_top,
            )
        )

        logging.info(f"Sending top for client {client_id}")
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, fruit_top_list])
        )

    def process_messsage(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                self._process_data(*fields)
            else:
                self._process_eof(fields[0])
            ack()
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            nack()

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self._closed = True
        self.input_queue.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.input_queue.start_consuming(self.process_messsage)

    def close(self):
        if not self._closed:
            self.input_queue.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    try:
        aggregation_filter.start()
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        aggregation_filter.close()
    return 0


if __name__ == "__main__":
    main()
