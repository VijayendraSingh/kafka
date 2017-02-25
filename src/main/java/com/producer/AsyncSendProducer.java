package com.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class AsyncSendProducer {



		public static void main(String[] args) throws InterruptedException, ExecutionException {
			// TODO Auto-generated method stub
			
			String topicName="MyTopic1";
			String key="key1";
			String value=" AsyncSendProducer puchhu kissi chahiye";
			Properties props=new Properties();
			props.put("bootstrap.servers", "192.168.0.107:9092,192.168.0.107:9093");
		      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");         
		      props.put("value.serializer", "org.apa"
		      		+ "che.kafka.common.serialization.StringSerializer");
		      
		      Producer<String, String> producer=new KafkaProducer<String, String>(props);
		      ProducerRecord<String, String> record=new ProducerRecord<String, String>(topicName, key,value);
		      Future<RecordMetadata> send = producer.send(record, new MyProducerCallBack());
		   
		      producer.close();
		      System.out.println("AsyncSendProducer program ended");
		}

}

class MyProducerCallBack implements Callback
{

	public void onCompletion(RecordMetadata metadata, Exception exception) {
		// TODO Auto-generated method stub
		if(exception!=null)
		{
			System.out.println("AsyncSendProducer failed with exception");
		}
		else
		{
			System.out.println("AsyncSendProducer call success");
		}
		
	}
	
}


