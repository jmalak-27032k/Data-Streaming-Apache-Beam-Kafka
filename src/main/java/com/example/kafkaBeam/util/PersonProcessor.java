package com.example.kafkaBeam.util;

import com.example.kafkaBeam.model.Person;
import org.apache.beam.sdk.values.KV;
import org.codehaus.jackson.map.ObjectMapper;

import java.time.LocalDate;
import java.time.Period;

public class PersonProcessor {
    private final String evenTopic = "EVEN_TOPIC";
    private final String oddTopic = "ODD_TOPIC";

    public KV<String, String> processPerson(Person person) {
        LocalDate dob = person.getDateOfBirth();
        int age = Period.between(dob, LocalDate.now()).getYears();
        String topic = (age % 2 == 0) ? evenTopic : oddTopic;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return KV.of(topic, objectMapper.writeValueAsString(person));
        } catch (Exception e) {
            throw new RuntimeException("Error converting Person to JSON", e);
        }
    }

}
