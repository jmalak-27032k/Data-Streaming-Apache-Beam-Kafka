package com.example.kafkaBeam.pipeline;

import com.example.kafkaBeam.model.Person;
import com.example.kafkaBeam.util.PersonProcessor;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaPipelineImplPipeline implements KafkaPipeline{
    @Override
    public void run(MyOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        String bootstrapServers = options.getBootstrapServers();
        String sourceTopic = options.getSourceTopic();
        String evenTopic = "EVEN_TOPIC";
        String oddTopic = "ODD_TOPIC";
        PersonProcessor personProcessor = new PersonProcessor();

        pipeline.apply(KafkaIO.<String, String>read()
                        .withBootstrapServers(bootstrapServers)
                        .withTopic(sourceTopic)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withoutMetadata())
                .apply(MapElements.via(new JsonToPersonFn()))
                .apply(ParDo.of(new PersonToKVFn(personProcessor)))
                .apply("Route to Topics", ParDo.of(new RouteToTopicFn()))
                .apply(KafkaIO.<String, String>writeRecords()
                        .withBootstrapServers(bootstrapServers)
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class));

        pipeline.run().waitUntilFinish();
    }

    // Inner class for converting JSON to Person object
    static class JsonToPersonFn extends SimpleFunction<KV<String, String>, Person> {
        @Override
        public Person apply(KV<String, String> input) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                return objectMapper.readValue(input.getValue(), Person.class);
            } catch (Exception e) {
                throw new RuntimeException("Error parsing JSON", e);
            }
        }
    }

    // Inner class for processing Person object and creating KV pairs
    static class PersonToKVFn extends DoFn<Person, KV<String, String>> {
        private final PersonProcessor personProcessor;

        public PersonToKVFn(PersonProcessor personProcessor) {
            this.personProcessor = personProcessor;
        }

        @ProcessElement
        public void processElement(ProcessContext context) {
            Person person = context.element();
            context.output(personProcessor.processPerson(person));
        }
    }

    // Inner class for routing KV pairs to appropriate topics
    static class RouteToTopicFn extends DoFn<KV<String, String>, ProducerRecord<String, String>> {
        private static final String EVEN_TOPIC = "EVEN_TOPIC";
        private static final String ODD_TOPIC = "ODD_TOPIC";

        @ProcessElement
        public void processElement(ProcessContext context) {
            KV<String, String> kv = context.element();
            String topic = kv.getKey();
            if ("even".equals(topic)) {
                context.output(new ProducerRecord<>(EVEN_TOPIC, kv.getValue()));
            } else if ("odd".equals(topic)) {
                context.output(new ProducerRecord<>(ODD_TOPIC, kv.getValue()));
            }
        }
    }
}




