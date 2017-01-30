package com.mazgon.kafka;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Command line tool for setting the current offset in internal Kafka topic '__consumer_offsets'
 *
 * Created by lovro on 30.1.2017.
 */
public class OffsetSetter {
    private static final String KAFKA_CONFIG_LONGOPT = "config";
    private static final String KAFKA_TOPIC_LONGOPT = "topic";
    private static final String KAFKA_PARTITION_LONGOPT = "partition";
    private static final String KAFKA_OFFSET_LONGOPT = "offset";

    private static final String DEFAULT_KAFKA_CONFIG = "kafka.properties";

    public static void main(String[] args) {
        OffsetSetterConfig config = null;

        try {
            config = createOffsetSetterConfig(args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        Map<TopicPartition, OffsetAndMetadata> m = new HashMap<>();
        m.put(new TopicPartition(config.kafkaTopic, config.kafkaPartition), new OffsetAndMetadata(config.kafkaOffset));

        System.out.println("Creating Kafka consumer ...");
        KafkaConsumer<String, String> kc = new org.apache.kafka.clients.consumer.KafkaConsumer<>(config.kafkaProperties);
        System.out.println("Committing offset " + config.kafkaOffset + " to topic " + config.kafkaTopic + ", partition " + config.kafkaPartition + " ...");
        kc.commitSync(m);
        System.out.println("Closing Kafka consumer ...");
        kc.close();
        System.out.println("Done!");
    }

    private static Options createCliOptions() {
        Options opts = new Options();
        opts.addOption("c", KAFKA_CONFIG_LONGOPT, true, "Path to the properties file for the Kafka consumer");
        opts.addOption("t", KAFKA_TOPIC_LONGOPT, true, "The topic for which to set the offset");
        opts.addOption("p", KAFKA_PARTITION_LONGOPT, true, "The partition for which to set the offset");
        opts.addOption("o", KAFKA_OFFSET_LONGOPT, true, "The custom offset, which will be set as the last offset consumed");
        return opts;
    }

    private static OffsetSetterConfig createOffsetSetterConfig(String[] args) throws ParseException {
        OffsetSetterConfig config = new OffsetSetterConfig();

        Options options = createCliOptions();

        CommandLine cli = new DefaultParser().parse(options, args, true);
        config.kafkaTopic = cli.getOptionValue(KAFKA_TOPIC_LONGOPT);
        String strKafkaPartition = cli.getOptionValue(KAFKA_PARTITION_LONGOPT);
        String strKafkaOffset = cli.getOptionValue(KAFKA_OFFSET_LONGOPT);

        if (config.kafkaTopic == null) throw new ParseException("Missing argument '" + KAFKA_TOPIC_LONGOPT + "'");
        if (strKafkaPartition == null) throw new ParseException("Missing argument '" + KAFKA_PARTITION_LONGOPT + "'");
        if (strKafkaOffset == null) throw new ParseException("Missing argument '" + KAFKA_OFFSET_LONGOPT + "'");

        try { config.kafkaPartition = Integer.parseInt(strKafkaPartition); } catch (NumberFormatException e) {
            throw new ParseException("Could not parse argument -" + KAFKA_PARTITION_LONGOPT + "='" + strKafkaPartition + "' because: " + e.getMessage());
        }
        try { config.kafkaOffset = Integer.parseInt(strKafkaOffset); } catch (NumberFormatException e) {
            throw new ParseException("Could not parse argument -" + KAFKA_OFFSET_LONGOPT + "='" + strKafkaPartition + "' because: " + e.getMessage());
        }

        String tempKafkaPropertiesPath = cli.getOptionValue(KAFKA_CONFIG_LONGOPT, DEFAULT_KAFKA_CONFIG);
        config.kafkaProperties = new Properties();

        try (InputStream input = new FileInputStream(tempKafkaPropertiesPath)) {
            // load a properties file
            config.kafkaProperties.load(input);
        } catch (IOException ex) {
            throw new ParseException("Could not parse properties file '" + tempKafkaPropertiesPath + "' because: " + ex.getMessage());
        }

        return config;
    }

    private static class OffsetSetterConfig {
        public Properties kafkaProperties;
        public String kafkaTopic;
        public int kafkaPartition;
        public int kafkaOffset;
    }
}
