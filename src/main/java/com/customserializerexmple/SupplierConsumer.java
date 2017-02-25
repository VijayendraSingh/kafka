package com.customserializerexmple;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SupplierConsumer {
	
	public static void main(String[] args) throws Exception{
		String topicName = "SensorTopic";
        String groupName = "SupplierTopicGroup";

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.107:9092,192.168.0.107:9093");
        props.put("group.id", groupName);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "com.customserializerexmple.SupplierDeserializer");


        KafkaConsumer<String, Supplier> consumer = new KafkaConsumer<String, Supplier>(props);
        consumer.subscribe(Arrays.asList(topicName));

        while (true){
                ConsumerRecords<String, Supplier> records = consumer.poll(100);
                for (ConsumerRecord<String, Supplier> record : records){
                        System.out.println("Supplier id= " + String.valueOf(record.value().getSupplierId()) + " Supplier  Name = " + record.value().getSupplierName() + " Supplier Start Date = " + record.value().getSupplierStartDate().toString());
                }
        }
	}

}
