package it.fabryprog.kafka.streaming.example.transformation.example;

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

public class WordCountTest extends AbstractTest implements TestInterface {

    @Test
    public void shouldTest() {
        final List<String> inputValues = Arrays.asList(
            "It was the best of times: it was the worst of times",
            "It is a sunny day"
        );

        // Step 0: Expected output is a couple of word -> count
        final List<KeyValue<String, Long>> expectedOutput = Arrays.asList(
            KeyValue.pair("it", 1L),
            KeyValue.pair("was", 1L),
            KeyValue.pair("the", 1L),
            KeyValue.pair("best", 1L),
            KeyValue.pair("of", 1L),
            KeyValue.pair("times", 1L),
            KeyValue.pair("it", 2L),
            KeyValue.pair("was", 2L),
            KeyValue.pair("the", 2L),
            KeyValue.pair("worst", 1L),
            KeyValue.pair("of", 2L),
            KeyValue.pair("times", 2L),
            KeyValue.pair("it", 3L),
            KeyValue.pair("is", 1L),
            KeyValue.pair("a", 1L),
            KeyValue.pair("sunny", 1L),
            KeyValue.pair("day", 1L)
        );

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), configuration)) {
            // Step 1: Setup input and output topics.
            final TestInputTopic<String, String> input = testDriver
                    .createInputTopic(inputTopic,
                            new StringSerializer(),
                            new StringSerializer());
            final TestOutputTopic<String, Long> output = testDriver
                    .createOutputTopic(outputTopic, new StringDeserializer(), new LongDeserializer());

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

        KTable<String, Long> wordCount = input.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+"))).
            peek((key, value) -> System.out.println("key=" + key + ", value=" + value)).
            groupBy((key, value) -> value, Grouped.with(Serdes.String(), Serdes.String())).
            count();

        wordCount.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        return builder;
    }
}
