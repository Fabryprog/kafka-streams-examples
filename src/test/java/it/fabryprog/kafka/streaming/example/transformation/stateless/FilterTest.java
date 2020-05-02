package it.fabryprog.kafka.streaming.example.transformation.stateless;

import it.fabryprog.kafka.streaming.example.AbstractTest;
import it.fabryprog.kafka.streaming.example.TestInterface;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * Transformation:
 *  - KStream → KStream
 *  - KTable → KTable
 *
 * Description:
 * Evaluates a boolean function for each element and retains those for which the function returns true.
 *
 */

public class FilterTest extends AbstractTest implements TestInterface {

    @Test
    public void shouldTest() {
        final List<Integer> inputValues = Arrays.asList(
                100, 200, 500, 1000, 9000, 100000
        );

        // Step 0: Expected output is all numbers less than 500 (inclusive)
        final List<Integer> expectedOutput = Arrays.asList(
                100, 200, 500
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
            input.pipeValueList(inputValues);

            // Step 3: Validate the output.
            assertThat(output.readValuesToList(), equalTo(expectedOutput));
        }
    }

    @Override
    public StreamsBuilder createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Integer> input = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.Integer()));
        KStream<String, Integer> filter = input.filter(
                (key, value) -> value <= 500
        );
        // starts with 'B'
        filter.to(outputTopic, Produced.with(Serdes.String(), Serdes.Integer()));
        return builder;
    }
}
