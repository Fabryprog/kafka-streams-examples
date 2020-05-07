package it.fabryprog.kafka.streaming.example.transformation.stateless;

import it.fabryprog.kafka.streaming.example.AbstractTest;
import it.fabryprog.kafka.streaming.example.TestInterface;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Transformation:
 *  - KStream → KStream
 *  - KTable → KTable
 *
 * Description:
 *  Takes one record and produces one record, while retaining the key of the original record.
 *  You can modify the record value and the value type.
 *
 *  mapValues is preferable to map because it will not cause data re-partitioning.
 *  However, it does not allow you to modify the key or key type like map does.
 *  Note that it is possible though to get read-only access to the input record key if you use ValueMapperWithKey instead of ValueMapper
 *
 */
public class MapValuesTest extends AbstractTest implements TestInterface {

    @Test
    public void shouldTest() {
        final List<String> inputValues = Arrays.asList(
                "A legend in the making".split(" ")
        );

        // Step 0: Expected output is all words (uppercase) with same key (instant)
        final List<KeyValue<Long, String>> expectedOutput = Arrays.asList(
                new KeyValue<>(null, "A"),
                new KeyValue<>(null, "LEGEND"),
                new KeyValue<>(null, "IN"),
                new KeyValue<>(null, "THE"),
                new KeyValue<>(null, "MAKING")
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

        KStream<String, Object> transformed = input.mapValues(
                value -> {
                    return value.toUpperCase();
                }
        );
        Serde<?> vSerdes = Serdes.String();

        transformed.to(outputTopic, Produced.with(Serdes.String(), (Serde<Object>)vSerdes));
        return builder;
    }
}
