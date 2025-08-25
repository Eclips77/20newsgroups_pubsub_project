
20 Newsgroups Pub/Sub System (Kafka + FastAPI + MongoDB)
=======================================================

Structure
---------
- publisher/
  - app/api.py          : FastAPI API for the publisher
  - app/producer.py     : Kafka producer logic
  - app/data_analyzer.py: Dataset loader & sampler (one per category)
  - app/config.py       : Service configuration via environment variables
- subscriber_interesting/
  - app/api.py          : FastAPI API for the 'interesting' subscriber
  - app/consumer.py     : Kafka consumer loop for 'interesting' topic
  - app/config.py       : Service configuration via environment variables
- subscriber_not_interesting/
  - app/api.py          : FastAPI API for the 'not_interesting' subscriber
  - app/consumer.py     : Kafka consumer loop for 'not_interesting' topic
  - app/config.py       : Service configuration via environment variables
