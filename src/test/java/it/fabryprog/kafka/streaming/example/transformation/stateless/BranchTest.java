package it.fabryprog.kafka.streaming.example.transformation.stateless;

import it.fabryprog.kafka.streaming.example.AbstractTest;
import it.fabryprog.kafka.streaming.example.TestInterface;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.*;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * Transformation:
 *  - KStream → KStream[]
 * Description:
 * Branch (or split) a KStream based on the supplied predicates into one or more KStream instances.
 * Predicates are evaluated in order. A record is placed to one and only one output stream on the first match: if the n-th predicate evaluates to true, the record is placed to n-th stream. If no predicate matches, the the record is dropped.
 * Branching is useful, for example, to route records to different downstream topics.
 *
 */

public class BranchTest extends AbstractTest implements TestInterface {

    @Test
    public void shouldTest() {
        final List<String> inputValues = Arrays.asList(
                "A head for business and a body for sin".split(" ")
        );

        // Step 0: Expected output is the words started by 'B'
        final List<String> expectedOutput = Arrays.asList(
                "business", "body"
        );

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), configuration)) {
            // Step 1: Setup input and output topics.
            final TestInputTopic<String, String> input = testDriver
                    .createInputTopic(inputTopic,
                            new StringSerializer(),
                            new StringSerializer());
            final TestOutputTopic<String, String> output = testDriver
                    .createOutputTopic(outputTopic, new StringDeserializer(), new StringDeserializer());

            // Step 2: Write the input.
            input.pipeValueList(inputValues);

            // Step 3: Validate the output.
            assertThat(output.readValuesToList(), equalTo(expectedOutput));
        }
    }

    @Override
    public StreamsBuilder createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> input = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));


        KStream<String, String>[] branches = input.branch(
                (key, value) -> value.toUpperCase().startsWith("A"),
                (key, value) -> value.toUpperCase().startsWith("B"),
                (key, value) -> true
        );
        // starts with 'B'
        branches[1].to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        return builder;
    }
}
