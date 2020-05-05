package it.fabryprog.kafka.streaming.example.transformation.stateless;

import it.fabryprog.kafka.streaming.example.AbstractTest;
import it.fabryprog.kafka.streaming.example.TestInterface;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * Transformation:
 * - KStream → KGroupedStream
 * - KTable → KGroupedTable
 *
 * Description:
 *  Groups the records by a new key, which may be of a different key type. When grouping a table, you may also specify a new value and value type.
 *  groupBy is a shorthand for selectKey(...).groupByKey(). (KStream details, KTable details)
 *  Grouping is a prerequisite for aggregating a stream or a table and ensures that data is properly partitioned (“keyed”) for subsequent operations.
 *  When to set explicit SerDes: Variants of groupBy exist to override the configured default SerDes of your application,
 *  which you must do if the key and/or value types of the resulting KGroupedStream or KGroupedTable do not match the configured default SerDes.
 *
 * Notes:
 *  Always causes data re-partitioning: groupBy always causes data re-partitioning.
 *  If possible use groupByKey instead, which will re-partition data only if required.
 */

public class GroupByTest extends AbstractTest implements TestInterface {

    @Test
    public void shouldTest() {
        final List<KeyValue<String, Integer>> inputValues = Arrays.asList(
                KeyValue.pair("STAND-A 1", 100),
                KeyValue.pair("STAND-B 1", 50),
                KeyValue.pair("STAND-C 1", 10),
                KeyValue.pair("STAND-A 2", 50),
                KeyValue.pair("STAND-B 2", 40),
                KeyValue.pair("STAND-C 2", 20)
        );

        // Step 0: Expected output is the sum of values with same STAND
        final List<Integer> expectedOutput = Arrays.asList(
            100, 50, 10, 150, 90, 30
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
        KTable<String, Integer> groupByAndReduce = input.groupBy(
            (key, value) -> key.split(" ")[0],
            Grouped.with(
                Serdes.String(), /* key (note: type was modified) */
                Serdes.Integer())  /* value */
        ).reduce(
            (v1, v2) -> {
                return v1 + v2;
            }
        );

        groupByAndReduce.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Integer()));
        return builder;
    }
}
