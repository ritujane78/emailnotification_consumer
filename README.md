# emailnotification_consumer

This is a simple project that demonstrates a consumer of "products-created-events-topic" topic. the consumer subscribes to this topic and receives all the messages in the topic.

The error of not receiving a valid json message by the kafka broker is handled by **ErrorHandlingDeserializer** class and the bad message is sent to a **Dead Letter Topic**, encoded in base64.

Integration testing is also performed on the "handle" method of "ProductCreatedEventHandler" class to make sure correct message is being consumed by the consumer.

If a message is consumed successfully, its productId and messageId is stored in H2 database (in memory) to avoid re-exploration og those explored messages (idempotency)
# TODOs
Run 3 kafka servers with ports 9092, 9094 and 9096 3 because there are 3 replicas created.
If on windows, you can run the servers through wsl2 with ubuntu as OS.
The IP address mentioned in application.properties and server property files is the IP addr of the Ubuntu instance. Make sure the IP is mentioned correctly everywhere.
Run this spring boot app.
Run the producer app "kafka_topics" to publish messages to the broker.

Note: The port in which the spring boot app is run is made dynamic so the multiple instances of consumer can run together.
