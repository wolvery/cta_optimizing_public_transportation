"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = ['PLAINTEXT://localhost:9092', 'PLAINTEXT://localhost:9093', 'PLAINTEXT://localhost:9094']
SCHEMA_REGISTRY_URL = 'http://localhost:8081'

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
        
        schema_registry = CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL)
        
        self.broker_properties = {
            'bootstrap.servers': BOOTSTRAP_SERVERS            
        }              

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
        
        self.producer = AvroProducer(self.broker_properties, schema_registry= SCHEMA_REGISTRY_URL)
        

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""        
        new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in [self.topic_name]]
        admin_client = AdminClient(self.broker_properties)
        processes_to_create = admin_client.create_topic(new_topics=new_topics)
        
        for topic, process in processes_to_create.items():
        try:
            process.result()  # The result itself is None
            logger.info("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""        
        self.producer.flush()
        logger.info("producer closed")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
