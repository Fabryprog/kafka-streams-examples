package it.fabryprog.kafka.streaming.example;

import io.confluent.common.utils.TestUtils;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public abstract class AbstractTest implements TestInterface {
    
    protected static final String inputTopic = "inputTopic";
    protected static final String outputTopic = "outputTopic";

    protected StreamsBuilder builder;
    protected Properties configuration;

    /**
     * Create the topology and its configuration.
     */
    @Before
    public void before() {
        builder = createTopology();
        configuration = createTopologyConfiguration();
    }

    private Properties createTopologyConfiguration() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
        // Use a temporary directory for storing state, which will be automatically removed after the test.
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

        return streamsConfiguration;
    }
}
