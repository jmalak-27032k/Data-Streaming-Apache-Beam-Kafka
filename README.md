## Data-Streaming-Apache-Beam-Kafka
#### Steps to run the project
    # Build the project:
    `mvn clean install`
    
    # Run the application, specifying the bootstrap_servers and source_topic:
    `mvn exec:java -Dexec.mainClass="com.example.kafkaBeam.KafkaPipelineImplPipeline" -Dexec.args="--bootstrapServers=localhost:9092 --sourceTopic=SOURCE_TOPIC"`
### Running the Tests
    `mvn test`