package com.mazgon.kafka;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import ch.qos.logback.classic.Level;

/**
 * Command line tool for setting the current offset in internal Kafka topic '__consumer_offsets'
 *
 * Created by lovro on 30.1.2017.
 */
public class OffsetSetter {
    private static final String CONFIG_LONGOPT    = "config";
    private static final String TOPIC_LONGOPT     = "topic";
    private static final String PARTITION_LONGOPT = "partition";
    private static final String OFFSET_LONGOPT    = "offset";
    private static final String INLINE_CONFIG_LONGOPT    = "inline-config";

    private static final String DEFAULT_CONFIG = "kafka.properties";

    private static final Logger log = LoggerFactory.getLogger(OffsetSetter.class);

    public static void main(String[] args) {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);

        OffsetSetterConfig config = null;

        try {
            config = createOffsetSetterConfig(args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        String groupId = config.properties.getProperty("group.id");

        log.info("Creating Kafka consumer ...");
        KafkaConsumer<String, String> kc = new KafkaConsumer<>(config.properties);
        log.info("---");
        if (config.offset < 0) {
            // select existing offset
            TopicPartition tp = new TopicPartition(config.topic, config.partition);
            kc.assign(Collections.singletonList(tp));
            log.info("Next offset for group {}, topic {}, partition {}: {}", groupId, config.topic, config.partition, kc.position(tp));
        } else {
            // set offset
            Map<TopicPartition, OffsetAndMetadata> m = new HashMap<>();
            m.put(new TopicPartition(config.topic, config.partition), new OffsetAndMetadata(config.offset));

            log.info("Committing offset {} to group {}, topic {}, partition {} ...", groupId, config.offset, config.topic, config.partition);
            kc.commitSync(m);
        }
        log.info("---");
        log.info("Closing Kafka consumer ...");
        kc.close();
        log.info("Done!");
    }

    private static Options createCliOptions() {
        Options opts = new Options();
        opts.addOption("c", CONFIG_LONGOPT, true, "Path to the properties file for the Kafka consumer");
        opts.addOption("t", TOPIC_LONGOPT, true, "The topic for which to set the offset");
        opts.addOption("p", PARTITION_LONGOPT, true, "The partition for which to set the offset");
        opts.addOption("o", OFFSET_LONGOPT, true, "The custom offset, which will be set as the last offset consumed");
        opts.addOption("i", INLINE_CONFIG_LONGOPT, true, "A property from the properties file for the Kafka consumer (takes precedence in case the properties file contains the same property)");
        return opts;
    }

    private static OffsetSetterConfig createOffsetSetterConfig(String[] args) throws ParseException {
        OffsetSetterConfig config = new OffsetSetterConfig();

        Options options = createCliOptions();

        CommandLine cli = new DefaultParser().parse(options, args, true);
        config.topic = cli.getOptionValue(TOPIC_LONGOPT);
        String strKafkaPartition = cli.getOptionValue(PARTITION_LONGOPT);
        String strKafkaOffset = cli.getOptionValue(OFFSET_LONGOPT);

        if (config.topic == null) throw new ParseException("Missing argument '" + TOPIC_LONGOPT + "'");
        if (strKafkaPartition == null) throw new ParseException("Missing argument '" + PARTITION_LONGOPT + "'");

        try { config.partition = Integer.parseInt(strKafkaPartition); } catch (NumberFormatException e) {
            throw new ParseException("Could not parse argument -" + PARTITION_LONGOPT + "='" + strKafkaPartition + "' because: " + e.getMessage());
        }
        if (strKafkaOffset != null) {
            try { config.offset = Integer.parseInt(strKafkaOffset); } catch (NumberFormatException e) {
                throw new ParseException("Could not parse argument -" + OFFSET_LONGOPT + "='" + strKafkaPartition + "' because: " + e.getMessage());
            }
            if (config.offset < 0) {
                throw new ParseException("Argument " + OFFSET_LONGOPT + " accepts only non-negative values");
            }
        } else {
            config.offset = -1;
        }

        boolean propertiesPathSupplied = cli.getOptionValue(CONFIG_LONGOPT) != null;
        String kafkaPropertiesPath = cli.getOptionValue(CONFIG_LONGOPT, DEFAULT_CONFIG);
        config.properties = new Properties();

        try (InputStream input = new FileInputStream(kafkaPropertiesPath)) {
            // load a properties file
            config.properties.load(input);
        } catch (IOException ex) {
            // throw error only if the option was really supplied
            if (propertiesPathSupplied) {
                throw new ParseException("Could not parse properties file '" + kafkaPropertiesPath + "' because: " + ex.getMessage());
            } else {
                log.warn("Didn't find default properties file: {}", DEFAULT_CONFIG);
            }
        }

        String[] inlineConfig = cli.getOptionValues(INLINE_CONFIG_LONGOPT);
        if (inlineConfig != null) {
            for (String opt : inlineConfig) {
                String[] tokens = opt.split("=");
                config.properties.setProperty(tokens[0], tokens[1]);
            }
        }

        return config;
    }

    private static class OffsetSetterConfig {
        public Properties properties;
        public String topic;
        public int partition;
        public int offset;
    }
}
