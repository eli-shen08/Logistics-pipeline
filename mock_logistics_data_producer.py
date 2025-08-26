import time
import random
import uuid
from datetime import datetime, timezone
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.
    """
    if err is not None:
        print(f"Delivery failed for User record {msg.key()}: {err}")
        return
    print(f"User record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    print("-"*100)


# Kafka and Schema Registry configuration
kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'IK7YXKPHGK2W6EJL',
    'sasl.password': 'cflt88nYXiXrUCTPrRL48bT0RCr/tCOF3xKDiAOzYWgDFZqitRWpaaeergbwrOVQ'
}

schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-777rw.asia-south2.gcp.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('GBBI2FLXOD4NEGAZ', 'cflttbgyRlZvceOD5RhCTl5i5+kr4QHRS2a2KHR0hjdJHpLCGh1jbSB244GfL78A')
})


# Fetch the latest schema dynamically
def get_latest_schema(subject):
    schema = schema_registry_client.get_latest_version(subject).schema.schema_str
    return AvroSerializer(schema_registry_client, schema)

# schem_str= get_latest_schema('logistics_stream-value')
# print(schem_str)

key_serializer = StringSerializer('utf_8')  # Serialize keys as UTF-8 strings

# Logistics data Producers
logistics_producer = SerializingProducer({**kafka_config,
                                        'key.serializer': key_serializer, 
                                        'value.serializer': get_latest_schema('logistics_stream-value')})


# # Valid McDonald's menu items
cities = [
    "New York, NY", "Los Angeles, CA", "Chicago, IL",
    "Houston, TX", "Phoenix, AZ", "Philadelphia, PA",
    "San Antonio, TX", "San Diego, CA", "Dallas, TX",
    "San Jose, CA", "Miami, FL", "Seattle, WA",
    "Denver, CO", "Boston, MA", "Atlanta, GA"
]

# Mock data generation
def generate_mock_logistics_data():
    utc_now = int(datetime.utcnow().timestamp() * 1000)


    for i in range(100):
        # Generate matching order and payment data
        order_id = str(uuid.uuid4())

        shipment_id = 'SH' + str(random.randint(10**(6-1), 10**6 - 1))

        origin, destination = random.sample(cities, 2)

        status  = random.choice(["In-transit", "Out-for-Delivery", "Delivered"])

        order_time = utc_now - random.randint(0, 24 * 60 * 60 * 1000)
        # 1. Convert ms → seconds
        seconds = order_time / 1000
        # 2. Convert seconds → datetime (UTC)
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
        # 3. Convert datetime → ISO 8601 string
        iso_str = dt.isoformat(timespec='seconds').replace('+00:00', 'Z')

        order_time = iso_str  # Random timestamp within 24 hours

        # Produce order
        logistics_producer.produce(
            topic='logistics_stream',
            key=shipment_id,
            value={
                "order_id": order_id,
                "shipment_id": shipment_id,
                "origin": origin,
                "destination": destination,
                "status": status,
                "timestamp": iso_str
            },
            on_delivery=delivery_report
        )
        logistics_producer.flush()
        print("X"*100)

        time.sleep(1)

# Generate and publish mock data
generate_mock_logistics_data()
print("Mock data successfully published.")

# shipment_id = f"SH{str(random.randint(10**(6-1), 10**6 - 1))}"

# origin, destination = random.sample(cities, 2)

# status  = random.choice(["In-transit", "Out-for-Delivery", "Delivered"])

# utc_now = int(datetime.utcnow().timestamp() * 1000)
# order_time = utc_now - random.randint(0, 24 * 60 * 60 * 1000)

# # 1. Convert ms → seconds
# seconds = order_time / 1000

# # 2. Convert seconds → datetime (UTC)
# dt = datetime.fromtimestamp(seconds, tz=timezone.utc)

# # 3. Convert datetime → ISO 8601 string
# iso_str = dt.isoformat(timespec='seconds').replace('+00:00', 'Z')

# print(f"{shipment_id}\n {origin} \n {destination} \n {status} \n {iso_str}")