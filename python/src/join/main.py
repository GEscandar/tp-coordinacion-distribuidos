import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])
SUM_EOF_QUEUE = f"{SUM_PREFIX}_eof_queue"


class JoinFilter:

    def __init__(self):
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.multi_consumer = middleware.MultiQueueConsumer(MOM_HOST)
        self.multi_consumer.add_queue(INPUT_QUEUE, self.process_messsage)
        self.multi_consumer.add_queue(SUM_EOF_QUEUE, self.process_eof_message)
        self.partial_tops_by_client = {}
        self.eof_received_by_client = set()
        self._closed = False

    def _process_result(self, client_id, fruit_top_list):
        if client_id not in self.partial_tops_by_client:
            self.partial_tops_by_client[client_id] = []
        self.partial_tops_by_client[client_id].append(fruit_top_list)
        self._check_and_send_final_top(client_id)

    def _process_sum_eof(self, client_id):
        if client_id in self.eof_received_by_client:
            return
        self.eof_received_by_client.add(client_id)
        logging.info(f"Received sum EOF for client {client_id}")
        self._check_and_send_final_top(client_id)

    def _check_and_send_final_top(self, client_id):
        tops_count = len(self.partial_tops_by_client.get(client_id, []))
        if tops_count < 1:
            return
        if client_id not in self.eof_received_by_client:
            return

        partial_tops = self.partial_tops_by_client.pop(client_id, [])

        merged = {}
        for partial_top in partial_tops:
            for fruit, amount in partial_top:
                if fruit in merged:
                    merged[fruit] = merged[fruit] + fruit_item.FruitItem(fruit, amount)
                else:
                    merged[fruit] = fruit_item.FruitItem(fruit, amount)

        fruit_items = list(merged.values())
        fruit_items.sort(reverse=True)
        top_size = min(TOP_SIZE, len(fruit_items))
        final_top = fruit_items[:top_size]

        final_top_list = list(
            map(
                lambda fi: (fi.fruit, fi.amount),
                final_top,
            )
        )

        logging.info(f"Sending final top for client {client_id}")
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, final_top_list])
        )

        self.eof_received_by_client.discard(client_id)

    def process_messsage(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 2 and isinstance(fields[1], list):
                self._process_result(fields[0], fields[1])
            else:
                pass
            ack()
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            nack()

    def process_eof_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            self._process_sum_eof(fields[0])
            ack()
        except Exception as e:
            logging.error(f"Error processing EOF message: {e}")
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
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    try:
        join_filter.start()
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        join_filter.close()
    return 0


if __name__ == "__main__":
    main()
