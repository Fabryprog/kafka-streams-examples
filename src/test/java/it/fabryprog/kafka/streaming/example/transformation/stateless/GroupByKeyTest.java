package it.fabryprog.kafka.streaming.example.transformation.stateless;

import it.fabryprog.kafka.streaming.example.AbstractTest;
import it.fabryprog.kafka.streaming.example.TestInterface;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.junit.Test;

import java.security.KeyPair;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * Transformation:
 * - KStream → KGroupedStream
 *
 * Description:
 *  Groups the records by the existing key.
 *  Grouping is a prerequisite for aggregating a stream or a table and ensures that data is properly partitioned (“keyed”) for subsequent operations.
 *
 *  When to set explicit SerDes: Variants of groupByKey exist to override the configured default SerDes of your application,
 *  which you must do if the key and/or value types of the resulting KGroupedStream do not match the configured default SerDes.
 *
 * Notes:
 *  Causes data re-partitioning if and only if the stream was marked for re-partitioning.
 *  groupByKey is preferable to groupBy because it re-partitions data only if the stream was already marked for re-partitioning.
 *  However, groupByKey does not allow you to modify the key or key type like groupBy does.
 */

public class GroupByKeyTest extends AbstractTest implements TestInterface {

    @Test
    public void shouldTest() {
        final List<KeyValue<String, Integer>> inputValues = Arrays.asList(
                KeyValue.pair("STAND-A", 100),
                KeyValue.pair("STAND-B", 50),
                KeyValue.pair("STAND-C", 10),
                KeyValue.pair("STAND-B", 50)
        );

        // Step 0: Expected output is the sum of values with same key
        final List<Integer> expectedOutput = Arrays.asList(
                100,
                50,
                10,
                100
        );

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), configuration)) {
            // Step 1: Setup input and output topics.
            final TestInputTopic<String, Integer> input = testDriver
                    .createInputTopic(inputTopic,
                            new StringSerializer(),
                            new IntegerSerializer());
            final TestOutputTopic<String, Integer> output = testDriver
                    .createOutputTopic(outputTopic, new StringDeserializer(), new IntegerDeserializer());

            // Step 2: Write the input.
            input.pipeKeyValueList(inputValues);

            // Step 3: Validate the output.
            assertThat(output.readValuesToList(), equalTo(expectedOutput));
        }
    }

    @Override
    public StreamsBuilder createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Integer> input = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.Integer()));
        KTable<String, Integer> groupByKeyAndReduce = input.groupByKey().reduce(
            (v1, v2) -> {
                return v1 + v2;
           }
        );

        groupByKeyAndReduce.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Integer()));
        return builder;
    }
}
