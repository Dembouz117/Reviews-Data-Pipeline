config = {
    "openai": {
        "api_key": "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"
    }
    ,"kafka": {
        # Required connection configs for Kafka producer, consumer, and admin
        "sasl.username": "UGD4EM32OZP5MR5Z",
        "sasl.password": "9Qm86VJlvvkZTZPkRVKgsT1TpNKDiWhYhbBcT0mf12Lb7sgcocch3Bn3pARe2UYs",
        "bootstrap.servers": "pkc-312o0.ap-southeast-1.aws.confluent.cloud:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "session.timeout.ms": 50000,
    },
    "schema_registry": {
        # for evolving schemas
        # Required connection configs for Confluent Cloud Schema Registry
        "url":"https://psrc-wrp99.us-central1.gcp.confluent.cloud",
        "basic.auth.user.info": "WTDV4GFJLPLXUVC6:OoA6OtITWwVPupqD9XzQuG7t7WPR5GFeHlJwQEs/5FH21ye8v0+0BspeHhZFig8H",
    }
}