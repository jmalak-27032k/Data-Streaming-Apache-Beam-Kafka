package com.example.kafkaBeam.pipeline;

import com.example.kafkaBeam.model.Person;
import com.example.kafkaBeam.util.PersonProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
class KafkaPipelineImplPipelineTest {
    @Test
    void testJsonToPersonFn() {
        KV<String, String> kv = KV.of("key", "{\"name\":\"Jam Jam\",\"address\":\"123 ABC St\",\"dateOfBirth\":\"2000-03-27\"}");
        KafkaPipelineImplPipeline.JsonToPersonFn jsonToPersonFn = new KafkaPipelineImplPipeline.JsonToPersonFn();

        Person person = jsonToPersonFn.apply(kv);
        assertEquals("Jam Jam", person.getName());
        assertEquals("123 ABC St", person.getAddress());
        assertEquals(LocalDate.of(1990, 1, 1), person.getDateOfBirth());
    }

    @Test
    void testPersonToKVFn() {
        PersonProcessor personProcessor = Mockito.mock(PersonProcessor.class);
        Mockito.when(personProcessor.processPerson(Mockito.any(Person.class)))
                .thenReturn(KV.of("even", "{\"name\":\"Jam Jam\",\"address\":\"123 ABC St\",\"dateOfBirth\":\"2000-03-27\"}"));

        KafkaPipelineImplPipeline.PersonToKVFn personToKVFn = new KafkaPipelineImplPipeline.PersonToKVFn(personProcessor);
        Person person = new Person();
        person.setName("Jam Jam");
        person.setAddress("123 ABC St");
        person.setDateOfBirth(LocalDate.of(1990, 1, 1));

        List<Person> persons = Arrays.asList(person);
        TestPipeline testPipeline = TestPipeline.create();
        PAssert.that(testPipeline.apply(Create.of(persons))
                        .apply(ParDo.of(personToKVFn)))
                .containsInAnyOrder(KV.of("even", "{\"name\":\"Jam Jam\",\"address\":\"123 ABC St\",\"dateOfBirth\":\"2000-03-27\"}"));

        testPipeline.run().waitUntilFinish();
    }

    @Test
    void testRouteToTopicFn() {
        KafkaPipelineImplPipeline.RouteToTopicFn routeToTopicFn = new KafkaPipelineImplPipeline.RouteToTopicFn();

        KV<String, String> kvEven = KV.of("even", "{\"name\":\"Jam Jam\",\"address\":\"123 ABC St\",\"dateOfBirth\":\"2000-03-27\"}");
        KV<String, String> kvOdd = KV.of("odd", "{\"name\":\"Jane Doe\",\"address\":\"456 Elm St\",\"dateOfBirth\":\"1991-02-01\"}");

        List<KV<String, String>> kvList = Arrays.asList(kvEven, kvOdd);
        TestPipeline testPipeline = TestPipeline.create();
        PAssert.that(testPipeline.apply(Create.of(kvList))
                        .apply(ParDo.of(routeToTopicFn)))
                .satisfies(records -> {
                    records.forEach(record -> {
                        if (record.topic().equals("EVEN_TOPIC")) {
                            assertEquals("{\"name\":\"Jam Jam\",\"address\":\"123 ABC St\",\"dateOfBirth\":\"2000-03-27\"}", record.value());
                        } else if (record.topic().equals("ODD_TOPIC")) {
                            assertEquals("{\"name\":\"Jane Doe\",\"address\":\"456 Elm St\",\"dateOfBirth\":\"1991-02-01\"}", record.value());
                        } else {
                            fail("Unexpected topic: " + record.topic());
                        }
                    });
                    return null;
                });

        testPipeline.run().waitUntilFinish();
    }
  
}