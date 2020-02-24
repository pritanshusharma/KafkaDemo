package com.KafkaTutorial;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        //producer property
        String bootstrapServer = "localhost:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        //High throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); //20ms wait time
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //32KB batch size

        //Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //Test Record
        ProducerRecord<String, String> record = new ProducerRecord<String,String>("test","Learning Kafka At Slalom");

        //Send Data Async
        //producer.send(record);

        //with callback
        producer.send(record, (recordMetadata, exception) -> {
            if(exception == null){
                System.out.println("Topic:"+recordMetadata.topic() + " Partition:" + recordMetadata.partition() + " Offset:" + recordMetadata.offset());
            }else{
                System.err.println(exception.getMessage());
            }
        });
        //flush the message and close producer
        producer.flush();
        producer.close();

    }
}
