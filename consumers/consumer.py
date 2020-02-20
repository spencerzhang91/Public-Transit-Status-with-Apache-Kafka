"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "public_transit_status": "http://localhost:8888",
            "landoop_kafka_connect_ui": "http://localhost:8084",
            "landoop_kafka_topics_ui": "http://localhost:8085",
            "landoop_schema_registry_ui": "http://localhost:8086",
            "kafka": "PLAINTEXT://localhost:9092, PLAINTEXT://localhost:9092, PLAINTEXT://localhost:9094",
            "rest_proxy": "http://localhost:8082",
            "schema_registry": "http://localhost:8081",
            "kafka_connect": "http://kafka-connect:8083",
            "ksql": "http://localhost:8088",
            "postgresql": "jdbc:postgresql://localhost:5432/cta"
        }

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema_registry"] = "http://localhost:8081"
            self.consumer = AvroConsumer(
                {
                    "bootstrap.servers": self.broker_properties["kafka"],
                    "auto.offset.reset": "earliest",
                    "group.id": "group.consumer.avro",
                    "schema.registry.url": self.broker_properties["shema_registry"]
                }
            )
        else:
            self.consumer = Consumer(
                {
                    "bootstrap.servers": self.broker_properties["kafka"],
                    "auto.offset.reset": "earliest",
                    "group.id": "group.consumer",
                }
            )

        #
        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        #
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        # logger.info("on_assign is incomplete - skipping")
        for partition in partitions:
            partition.offset = OFFSET_BEGINNING
        
        consumer.assign(partitions)
        logger.info("partitions assigned for %s", self.topic_name_pattern)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        #
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        #
        #
        # logger.info("_consume is incomplete - skipping")
        try:
            message = self.consumer.poll()
        except RuntimeError as e:
            print(f"Error occured! \n{e}\n")
        if not message:
            return 0
        else:
            return 1


    def close(self):
        """Cleans up any open kafka consumers"""
        #
        #
        # TODO: Cleanup the kafka consumer
        #
        #
        self.consumer.close()
