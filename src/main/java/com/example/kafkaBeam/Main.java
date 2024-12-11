package com.example.kafkaBeam;

import com.example.kafkaBeam.pipeline.KafkaPipeline;
import com.example.kafkaBeam.pipeline.KafkaPipelineImplPipeline;
import com.example.kafkaBeam.pipeline.MyOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class Main {
    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        KafkaPipeline kafkaPipeline = new KafkaPipelineImplPipeline();
        try {
            kafkaPipeline.run(options);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
