# kafka-offset-setter
CLI tool for changing the last consumed offset of a Kafka consumer.

### Example usage
Our consumer is defined in **kafka.properties** and we want to set the offset to **12345** in the topic **MY_TOPIC**, partition **2**. Just issue the command:

    java -jar kafka-offset-setter-0.0.1.jar -t MY_TOPIC -p 2 -c kafka.properties -o 12345
    
Expected output:

    Creating Kafka consumer ...
    Committing offset 12345 to topic MY_TOPIC, partition 2 ...
    Closing Kafka consumer ...
    Done!

Next time you will start your consumer, it will start at offset **12346**.
