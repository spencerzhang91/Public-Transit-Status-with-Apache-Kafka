"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=5,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://kafka0:9092",
            "schema.registry.url": "http://schema-registry:8081/",
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )


    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        # Creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        logger.info(f"Beginning topic creation for {self.topic.name}")
        client = AdminClient(
            {"bootstrap.servers": self.broker_properties["bootstrap.servers"]}
        )
        topic_exists = self.chech_topic_exists(client, self.topic_name)
        
        if topic_exists:
            logger.info(f"Topic {self.topic_name} already exist. Creation aborted.")
            return

        logger.info(f"Creating topic: {self.topic_name}")

        # Using `client`, create the topic
        # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.AdminClient.create_topics
        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas
                )
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic {self.topic_name} created")
            except Exception as e:
                logger.fatal(f"Failed to create topic {topic}: {e}")


    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        # Write cleanup code for the Producer
        #
        if self.producer is not None:
            logger.debug("Flushing producer...")
            self.producer.flush()


    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

    
    def chech_topic_exists(self, client, topic_name):
        """Checks if topic already exists."""
        topic_metadata = client.list_topics()
        topics = topic_metadata.topics
        return topic_name in topics

