import base64
import yaml
import random
import json
from faker import Faker
from typing import Dict, List
from datetime import date


def load_event_config(path="data_creation/event_config.yml"):
    with open(path, "r") as file:
        return yaml.safe_load(file)


event_config = load_event_config()

fake = Faker(
    ["de_DE", "fr_FR", "en_UK", "pl_PL", "pt_PT", "de_CH", "de_AT", "it_IT", "es_ES"]
)


# Recursively generate fields based on the event type
def generate_fields(fields_config):
    """
    Generates fields based on the given fields configuration.

    Parameters:
        fields_config (dict): A dictionary containing the configuration for generating fields.
            The keys represent the field names, and the values represent the field types.

    Returns:
        dict: A dictionary containing the generated fields. The keys represent the field names,
            and the values represent the generated values based on the field types.

    """
    # First set up general fields for all event types
    fields = {}
    # Then go through the config and generate event_type-specific fields
    for field, field_type in fields_config.items():
        if (
            field_type == "account_id"
            or field_type == "session_id"
            or field_type == "user_id"
            or field_type == "exercise_id"
            or field_type == "lesson_id"
        ):
            fields[field] = str(fake.uuid4())
        elif field_type == "currency":
            fields[field] = fake.currency_code()
        elif field_type == "payment_type":
            fields[field] = random.choice(["credit_card", "paypal", "bank_transfer"])
        elif field_type == "registration_method":
            fields[field] = random.choice(["Google", "Microsoft", "Facebook", "Apple"])
        elif field_type == "number":
            fields[field] = fake.random_int(min=1, max=100)
        elif field_type == "number_1to10":
            fields[field] = fake.random_int(min=1, max=10)
        elif field_type == "timestamp":
            fields[field] = fake.iso8601()
        elif field_type == "device":
            fields[field] = random.choice(["Android", "iOS", "web"])
        elif field_type == "language_id":
            fields[field] = "lang_" + random.choice(
                ["de", "it", "es", "fr", "en", "pl", "pt", "ru", "tr", "zh"]
            )
        elif field_type == "duration":
            fields[field] = fake.random_int(min=1, max=120)
        elif field_type == "email":
            fields[field] = fake.email()
        elif field_type == "sentence":
            fields[field] = fake.sentence()
        elif field_type == "rating":
            fields[field] = fake.random_int(min=1, max=5)
        elif field_type == "score":
            fields[field] = fake.random_int(min=0, max=100)
        elif field_type == "difficulty":
            fields[field] = random.choice(["Easy", "Medium", "Hard"])
        elif field_type == "amount":
            fields[field] = fake.random_number(digits=2)
        elif field_type == "location":
            fields[field] = f"{fake.city_name()}, {fake.current_country()}"
        elif field_type == "not_applicable":
            fields[field] = f"not_applicable"
        elif field_type == "campaign_id":
            fields[field] = "camp_" + str(fake.random_int(min=1000, max=9999))
    return fields


# Function to generate event based on the event type
def generate_event(event_type, event_subtype):
    fields_config = event_config[event_type][event_subtype]["fields"]
    event = {
        "event_uuid": str(fake.uuid4()),
        # creation date as UNIX timestamp
        "created_at": fake.unix_time(
            start_datetime=date(2024, 3, 1), end_datetime=date(2024, 4, 26)
        ),
        "event_name": f"{event_type}:{event_subtype}",
        "event_specifics": generate_fields(fields_config),
    }
    return event


def generate_random_event_type_plus_event():
    random_event_type = random.choice(list(event_config.keys()))
    corresponding_event_subtype = random.choice(list(event_config[random_event_type]))

    # Generate a full event for the above event_type und _subtype
    random_event = generate_event(random_event_type, corresponding_event_subtype)
    return random_event


# This is the official Kinesis JSON example from AWS, slightly adapted by me
KINESIS_JSON_TEMPLATE = """
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "1",
                "sequenceNumber": "49590338271490256608559692538361571095921575989136588898",
                "data": "DATA_HERE",
                "approximateArrivalTimestamp": 1545084650.987
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "shardId-000000000006:49590338271490256608559692538361571095921575989136588898",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::123456789012:role/lambda-role",
            "awsRegion": "eu-central-1",
            "eventSourceARN": "arn:aws:kinesis:eu-central-1:123456789012:stream/lambda-stream"
        }
"""


def generate_events_json(n) -> str:
    event_list = []
    for _ in range(n):
        random_event = generate_random_event_type_plus_event()
        event_list.append(random_event)
    return json.dumps(event_list, ensure_ascii=False, indent=4)


def toy_data_generation(n: int = 1000):
    """
    DATA GENERATION: Create Event and bundle it to Kinesis JSON as Base64

    Args:
        n (int, optional): Number of events to generate. Defaults to 1000.

    Returns:
        Dict[str, List[str]]: Kinesis stream records in a dictionary with Records as value.
    """
    kinesis_dict: Dict[str, List[str]] = {"Records": []}

    for _ in range(n):
        testevent = generate_random_event_type_plus_event()
        testevent_b64 = base64.b64encode(json.dumps(testevent).encode("utf-8"))

        kinesis_dict["Records"].append(
            KINESIS_JSON_TEMPLATE.replace("DATA_HERE", testevent_b64.decode("utf-8"))
        )

    ## Artificially create duplicate
    if random.random() < 0.05:
        for _ in range(random.randint(1, 10)):
            index = random.randint(0, len(kinesis_dict["Records"]) - 1)
            kinesis_dict["Records"].append(kinesis_dict["Records"][index])
    return kinesis_dict


if __name__ == "__main__":
    sample_json = generate_events_json(500)
    with open("output/sample_events.json", "w", encoding="utf-8") as f:
        f.write(sample_json)
