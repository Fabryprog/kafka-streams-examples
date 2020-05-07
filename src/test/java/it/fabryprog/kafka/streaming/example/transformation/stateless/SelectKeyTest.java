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
 * - KStream → KStream
 *
 * Description:
 *  Assigns a new key – possibly of a new key type – to each record. (details)
 *  Calling selectKey(mapper) is the same as calling map((key, value) -> mapper(key, value), value).
 *
 *  Marks the stream for data re-partitioning:
 *  Applying a grouping or a join after selectKey will result in re-partitioning of the records.
 */

public class SelectKeyTest extends AbstractTest implements TestInterface {

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
        KTable<String, Integer> selectKeyAndGroupByKeyAndReduce = input.
            selectKey((key, value) -> key.split(" ")[0]).
            groupByKey(
                Grouped.with(
                    Serdes.String(),
                    Serdes.Integer()
                )
            ).
            reduce(
                (v1, v2) -> {
                    return v1 + v2;
                }
            );

        selectKeyAndGroupByKeyAndReduce.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Integer()));
        return builder;
    }
}
