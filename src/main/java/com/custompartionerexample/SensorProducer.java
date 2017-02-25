package com.custompartionerexample;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SensorProducer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String topicName="SensorTopic";
		
		
		Properties props=new Properties();
		//mandatory properties
		props.put("bootstrap.servers", "192.168.0.107:9092,192.168.0.107:9093,192.168.0.107:9094");
	    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");         
	    props.put("value.serializer", "org.apa"
	      		+ "che.kafka.common.serialization.StringSerializer");
	     //extra properties
	      props.put("partitioner.class", "com.custompartionerexample.SensorPartitioner");
	      props.put("speed.sensor.name", "TSS");
	      
	      Producer<String, String> producer=new KafkaProducer<String, String>(props);
	      
	      for (int i = 0; i < 10; i++) {
	     	  System.out.println("sending data with key SSP and value as "+"500"+i);
	    	  producer.send(new ProducerRecord<String, String>(topicName, "SSP"+i, "500"+i));	
		}
	      
	      
	      for (int i = 0; i < 10; i++) {
	    	  System.out.println("sending data with key TSS and value as "+"500"+i);
	    	  producer.send(new ProducerRecord<String, String>(topicName, "TSS", "500"+i));	
		}
	      
	     
	      producer.close();
	      System.out.println("producer ended");

	}

}
