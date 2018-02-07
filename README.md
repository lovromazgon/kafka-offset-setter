# kafka-offset-setter
CLI tool for changing the last consumed offset of a Kafka consumer.

### Example usage
Our consumer is defined in **kafka.properties** and we want to set the offset to **12345** in the topic **MY_TOPIC**, partition **2**. Just issue the command:

    java -jar kafka-offset-setter-0.0.2.jar -t MY_TOPIC -p 2 -c kafka.properties -o 12345
    
Expected output:

    Creating Kafka consumer ...
    Committing offset 12345 to topic MY_TOPIC, partition 2 ...
    Closing Kafka consumer ...
    Done!

Next time you will start your consumer, it will start at offset **12345**.

If you don't supply the option `-o`, then the program will just connect to Kafka and otput the current offset for the `group.id`.

You also have the possibility to supply properties for the Kafka consumer inline with `-i`

    java -jar kafka-offset-setter-0.0.2.jar -t MY_TOPIC -p 2 -i "group.id=my_group_id" -i "client.id=my_client_id"
