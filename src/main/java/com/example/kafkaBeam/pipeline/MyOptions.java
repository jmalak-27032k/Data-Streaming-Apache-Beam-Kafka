package com.example.kafkaBeam.pipeline;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {
    @Description("Kafka Bootstrap Servers")
    String getBootstrapServers();
    void setBootstrapServers(String bootstrapServers);

    @Description("Source Kafka Topic")
    String getSourceTopic();
    void setSourceTopic(String sourceTopic);
}
