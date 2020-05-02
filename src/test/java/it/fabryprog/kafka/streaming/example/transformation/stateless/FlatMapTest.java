package it.fabryprog.kafka.streaming.example.transformation.stateless;

import it.fabryprog.kafka.streaming.example.AbstractTest;
import it.fabryprog.kafka.streaming.example.TestInterface;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * Transformation:
 *  - KStream → KStream
 *
 * Description:
 * Takes one record and produces zero, one, or more records. You can modify the record keys and values, including their types.
 * Marks the stream for data re-partitioning: Applying a grouping or a join after flatMap will result in re-partitioning of the records.
 * If possible use flatMapValues instead, which will not cause data re-partitioning.
 *
 */

public class FlatMapTest extends AbstractTest implements TestInterface {

    private Long instant;

    @Before
    public void init() {
        instant = Instant.now().getEpochSecond();
    }

    @Test
    public void shouldTest() {
        final List<String> inputValues = Arrays.asList(
                "A legend in the making".split(" ")
        );

        // Step 0: Expected output is all words (uppercase) with same key (instant)
        final List<KeyValue<Long, String>> expectedOutput = Arrays.asList(
                new KeyValue<>(instant, "A"),
                new KeyValue<>(instant, "LEGEND"),
                new KeyValue<>(instant, "IN"),
                new KeyValue<>(instant, "THE"),
                new KeyValue<>(instant, "MAKING")
        );

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), configuration)) {
            // Step 1: Setup input and output topics.
            final TestInputTopic<String, String> input = testDriver
                    .createInputTopic(inputTopic,
                            new StringSerializer(),
                            new StringSerializer());
            final TestOutputTopic<Long, String> output = testDriver
                    .createOutputTopic(outputTopic, new LongDeserializer(), new StringDeserializer());

            // Step 2: Write the input.
            input.pipeValueList(inputValues);

            // Step 3: Validate the output.
            assertThat(output.readKeyValuesToList(), equalTo(expectedOutput));
        }
    }

    @Override
    public StreamsBuilder createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> input = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<Object, Object> transformed = input.flatMap(
            (key, value) -> {
                List<KeyValue<Long, String>> result = new LinkedList<>();
                result.add(KeyValue.pair(instant, value.toUpperCase()));
                return result;
            }
        );
        Serde<?> kSerdes = Serdes.Long();
        Serde<?> vSerdes = Serdes.String();

        transformed.to(outputTopic, Produced.with((Serde<Object>)kSerdes, (Serde<Object>)vSerdes));
        return builder;
    }
}
