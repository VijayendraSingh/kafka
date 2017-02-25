package com.customserializerexmple;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

public class SupplierSerializer implements Serializer<Supplier> {
	 private String encoding = "UTF8";

	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub

	}

	public void close() {
		// TODO Auto-generated method stub

	}

	public byte[] serialize(String topic, Supplier data) {
		// TODO Auto-generated method stub
		byte[] serialzedName;
		byte[] serializedDate;
		int sizeOfName;
		int sizeOfDate;
	
		
		try {
			if(data==null)
				return null;
			serialzedName=data.getSupplierName().getBytes(encoding);
			sizeOfName=serialzedName.length;
			serializedDate=data.getSupplierStartDate().toString().getBytes(encoding);
			sizeOfDate=serializedDate.length;
			ByteBuffer buf=ByteBuffer.allocate(4+4+sizeOfName+4+sizeOfDate);
			buf.putInt(data.getSupplierId());
			buf.putInt(sizeOfName);
			buf.put(serialzedName);
			buf.putInt(sizeOfDate);
			buf.put(serializedDate);
			
			return buf.array();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}

}
