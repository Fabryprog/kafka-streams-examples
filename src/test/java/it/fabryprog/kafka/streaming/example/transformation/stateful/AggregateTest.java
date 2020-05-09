package it.fabryprog.kafka.streaming.example.transformation.stateful;

import it.fabryprog.kafka.streaming.example.AbstractTest;
import it.fabryprog.kafka.streaming.example.TestInterface;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Transformation:
 *  - KGroupedStream → KTable
 *  - CogroupedKStream → KTable
 *  - KGroupedTable → KTable
 *
 * Description:
 * Rolling aggregation. Aggregates the values of (non-windowed) records by the grouped key.
 *
 * Aggregating is a generalization of reduce and allows, for example, the aggregate value to have a different type than the input values.
 * When aggregating a grouped stream, you must provide an:
 *  - initializer (e.g., aggValue = 0)
 *  - “adder” aggregator (e.g., aggValue + curValue).
 *
 *  When aggregating a cogrouped stream, you must only provide an initializer; the corresponding “adder” aggregators are provided in the prior cogroup() calls already.
 *
 *  When aggregating a grouped table, you must provide an initialzier, “adder”, and “subtractor” (think: aggValue - oldValue).
 *
 * Several variants of aggregate exist, see Javadocs for details.
 */
public class AggregateTest extends AbstractTest implements TestInterface {

    @Test
    public void shouldTest() {
        final List<KeyValue<String, Long>> inputValues = Arrays.asList(
                //STAND DOOR -> visitors
                KeyValue.pair("STAND A 1", 100L),
                KeyValue.pair("STAND A 2", 50L),
                KeyValue.pair("STAND A 3", 20L),
                KeyValue.pair("STAND B 1", 10L),
                KeyValue.pair("STAND B 2", 5L),
                KeyValue.pair("STAND B 3", 2L)
        );

        // Step 0: Expected sum of visitors for every STAND
        final List<Long> expectedOutput = Arrays.asList(
                100L, 150L, 170L, 10L, 15L, 17L
        );

        try(final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), configuration)) {
            // Step 1: Setup input and output topics.
            final TestInputTopic<String, Long> input = testDriver
                    .createInputTopic(inputTopic,
                            new StringSerializer(),
                            new LongSerializer());
            final TestOutputTopic<String, Long> output = testDriver
                    .createOutputTopic(outputTopic, new StringDeserializer(), new LongDeserializer());

            // Step 2: Write the input.
            input.pipeKeyValueList(inputValues);

            // Step 3: Validate the output.
            assertThat(output.readValuesToList(), equalTo(expectedOutput));
        }
    }

    @Override
    public StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Long> input = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.Long()));

        KTable<String, Long> aggregateStream = input.groupBy(
                (key, value) -> key.split(" ")[0] + key.split(" ")[1],
                Grouped.with(
                        Serdes.String(),
                        Serdes.Long())
        ).aggregate(
            () -> 0L, /** INITIALIZER **/
            (key, newValue, aggValue) -> newValue + aggValue, /** ADDER **/
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregate-test").withValueSerde(Serdes.Long()) /** STATE STORE NAME +  SERDES **/
        );

        aggregateStream.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        return builder;
    }

}
