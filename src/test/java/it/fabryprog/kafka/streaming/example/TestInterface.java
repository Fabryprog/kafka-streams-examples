package it.fabryprog.kafka.streaming.example;

import org.apache.kafka.streams.StreamsBuilder;
import org.junit.Test;

public interface TestInterface {
    public StreamsBuilder createTopology();

    @Test
    public void shouldTest();
}
