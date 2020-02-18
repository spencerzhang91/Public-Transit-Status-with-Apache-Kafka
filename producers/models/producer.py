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
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        self.broker_properties = {
            "public_transit_status": "http://localhost:8888",
            "landoop_kafka_connect_ui": "http://localhost:8084",
            "landoop_kafka_topics_ui": "http://localhost:8085",
            "landoop_schema_registry_ui": "http://localhost:8086",
            "kafka": [
                "PLAINTEXT://localhost:9092",
                "PLAINTEXT://localhost:9093",
                "PLAINTEXT://localhost:9094"
            ],
            "rest_proxy": "http://localhost:8082",
            "schema_registry": "http://localhost:8081",
            "kafka_connect": "http://localhost:8083",
            "ksql": "http://localhost:8088",
            "postgresql": "jdbc:postgresql://localhost:5432/cta"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            {
                "bootstrap.servers": self.broker_properties["kafka"],
                "compression.type": "lz4"
            },
            schema_registry=self.broker_properties["schema_registry"]
        )


    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.

        # Configure the AdminClient with `bootstrap.servers`
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.AdminClient
        client = AdminClient({"bootstrap.servers": self.broker_properties["kafka"]})

        # Create a NewTopic object. Don't forget to set partitions and replication factor to 1!
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.NewTopic
        topic = NewTopic(self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)

        # Using `client`, create the topic
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.AdminClient.create_topics
        try:
            client.create_topics([topic])
        except Exception as e:
            logger.info(f"Exception {e} occured. Topic creation failed.")
        else:
            logger.info(f"Topic {self.topic_name} created!")


    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        # TODO: Write cleanup code for the Producer here
        #
        try:
            # clean up logic here
            self.producer.flush()
            client = AdminClient({"bootstrap.servers": self.broker_properties["kafka"]})
            client.delete_topic([self.topic_name])
            Producer.existing_topics.remove(self.topic_name)

        except Exception as e:
            logger.info(f"Producer closing failed due to exeption {e}.")
        else:
            logger.info(f"Producer of topic name {self.topic_name} completed.")


    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))



if __name__ == "__main__":

    test_schema = {}
    test_producer = Producer("test_topic", test_schema)
