package it.fabryprog.kafka.streaming.example.transformation.stateful.example;

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
                "Love means never having to say you're sorry"
        );

        // Step 0: Expected output is word count
        final List<Long> expectedOutput = Arrays.asList(
                12L, 8L
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
            assertThat(output.readValuesToList(), equalTo(expectedOutput));
        }
    }

    @Override
    public StreamsBuilder createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> input = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, Long> wordCount = input.map((key, value) -> KeyValue.pair(value, Arrays.asList(value.split("\\W+")))).
            peek((key, value) -> System.out.println("key=" + key + ", value=" + value)).
                mapValues(value -> value.size()).
                groupByKey(Grouped.with(Serdes.String(), Serdes.Integer())).
                count();

        wordCount.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        return builder;
    }
}
