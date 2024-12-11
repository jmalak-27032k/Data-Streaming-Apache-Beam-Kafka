package com.example.kafkaBeam.pipeline;

public interface KafkaPipeline {
    void run(MyOptions options) throws Exception;
}
