package com.customserializerexmple;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SupplierProducer {

	public static void main(String[] args) throws InterruptedException, ExecutionException, ParseException {
		// TODO Auto-generated method stub

		String topicName = "SensorTopic";

		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.0.107:9092,192.168.0.107:9093");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer","com.customserializerexmple.SupplierSerializer");

		Producer<String, Supplier> producer = new KafkaProducer<String, Supplier>(props);

		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Supplier sup1 = new Supplier(101, "puchhu pvt. ltd", df.parse("2017-04-01"));
		Supplier sup2 = new Supplier(102, "kuchhu pvt. ltd", df.parse("2017-03-02"));

		RecordMetadata recordMetadata1 = producer.send(new ProducerRecord<String, Supplier>(topicName, "SUP", sup1)).get();
		System.out.println(" recordMetadata1 partition = "+recordMetadata1.partition() +" offset = "+recordMetadata1.offset());
		RecordMetadata recordMetadata2 =producer.send(new ProducerRecord<String, Supplier>(topicName, "SUP", sup2)).get();
		System.out.println(" recordMetadata2 partition = "+recordMetadata2.partition() +" offset = "+recordMetadata2.offset());

		System.out.println("supplier producer completed");
		producer.close();

	}

}
