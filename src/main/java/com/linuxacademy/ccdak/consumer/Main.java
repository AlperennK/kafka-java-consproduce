package com.linuxacademy.ccdak.consumer;

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class Main {
    public static Properties setProperties(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        return props;
    }

    public static void produceToKafka() {

        Properties props=setProperties();

        Producer<String, String> producer = new KafkaProducer<>(props);
        try {
            File file = new File(Main.class.getClassLoader().getResource("sample_transaction_log.txt").getFile());
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                String[] lineArray = line.split(":");
                String key = lineArray[0];
                String value = lineArray[1];
                producer.send(new ProducerRecord<>("inventory_purchases", key, value));
                if (key.equals("apples")) {
                    producer.send(new ProducerRecord<>("apple_purchases", key, value));
                }


            }
            br.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        producer.close();


    }

    public static void consumeKafka() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "group1");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("inventory_purchases"));
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("output.dat", true));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String recordString = "key=" + record.key() + ", value=" + record.value() + ", topic=" + record.topic() + ", partition=" + record.partition() + ", offset=" + record.offset();
                    System.out.println(recordString);
                    writer.write(recordString + "\n");
                }
                consumer.commitSync();
                writer.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);


        }
    }

    public static void main(String[] args) {
        produceToKafka();
        //consumeKafka();


    }
}

