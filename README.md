Setting up distributed task queues (like Celery) requires heavy central infrastructure
(Redis, RabbitMQ, Worker Nodes). For edge computing (e.g., hundreds of Raspberry Pis deployed in the field),
maintaining a central message broker is a single point of failure and requires massive configuration overhead
