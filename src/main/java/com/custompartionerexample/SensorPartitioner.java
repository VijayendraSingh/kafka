package com.custompartionerexample;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.*;



public class SensorPartitioner implements Partitioner {
	
	private String speedSensorName;

	public void configure(Map<String, ?> configs) {
		speedSensorName=configs.get("speed.sensor.name").toString();

	}

	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		// TODO Auto-generated method stub
		System.out.println(" sensor partitioner : key = "+ key+ "  value = "+value);
		List<PartitionInfo> partitionsForTopic = cluster.partitionsForTopic(topic);
		int totalNumOfPartitions=partitionsForTopic.size();
		int sp=(int)Math.abs(totalNumOfPartitions*0.3);
		int p=0;
		
		if((keyBytes==null)||(!(key instanceof String)))
		{
			throw new InvalidRecordException("All messages must have sensor name as a key");
		}
		if(((String)key).equals(speedSensorName))
		{
			p=Math.abs((Utils.murmur2(valueBytes))%sp);
			System.out.println("sensorname = "+(String)(key) + " partition num ="+p);
		}
		else
		{
			p=Math.abs((Utils.murmur2(keyBytes)%(totalNumOfPartitions - sp)))+ sp;
			System.out.println("sensorname = "+(String)(key) + " partition num ="+p);
		}
		
		System.out.println( " ......................................................  ");
		return p;
	}

	public void close() {
		// TODO Auto-generated method stub

	}

}
