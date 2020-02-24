package com.KafkaTutorial;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
public class ConsumerDemo {

    public static void main(String[] args) throws InterruptedException, ExecutionException{
        // System.out.println("Hello World");
        //consumer property
        String bootstrapServer = "localhost:9092";
        String groupId = "my-consumer-group";
        String topic = "test";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Subscribe consumer to topic(s)
        consumer.subscribe(Collections.singleton(topic));


        //Poll
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record: records){
                System.out.println(record.key() + record.value());
                System.out.println("TopicName:"+ record.topic() + " partition:"+record.partition() + " Offset:"+record.offset());
            }

            //Commit consumer offset (Async and manually)
            consumer.commitAsync();
        }
    }
}
