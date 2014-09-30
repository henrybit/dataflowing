package com.meetyou.dataflowing.kafka;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TestProducer {

	 public static void main(String[] args) {
		 	//./kafka-console-consumer.sh --zookeeper zoo35:2181/kafka-0.8.1.1 --topic testLog --from-beginning
	        long events = 50000;
	        Random rnd = new Random();
	 
	        Properties props = new Properties();
	        props.put("metadata.broker.list", "zoo35:9092,zoo36:9092");
	        props.put("serializer.class", "kafka.serializer.StringEncoder");
	        //props.put("partitioner.class", "example.producer.SimplePartitioner");
	        props.put("request.required.acks", "1");
	 
	        ProducerConfig config = new ProducerConfig(props);
	 
	        Producer<String, String> producer = new Producer<String, String>(config);
	 
	        for (long nEvents = 0; nEvents < events; nEvents++) { 
	               long runtime = new Date().getTime();  
	               String ip = "192.168.2." + rnd.nextInt(255); 
	               String msg = runtime + ",www.example.com," + ip; 
	               KeyedMessage<String, String> data = new KeyedMessage<String, String>("testLog", ip, msg);
	               producer.send(data);
	               System.out.println("send "+nEvents);
	        }
	        producer.close();
	    }
	 
	 
	 public static void test(String[] args) throws Exception {
			// TODO Auto-generated method stub
			Properties props = new Properties();
			props.put("metadata.broker.list", "zoo35:9092,zoo36:9092,zoo37:9092,zoo38:9092");
			props.put("serializer.class", "kafka.serializer.StringEncoder");
			//props.put("partitioner.class", "example.producer.SimplePartitioner");
			props.put("request.required.acks", "1");
			
			String key = (args!=null&&args.length>=2)?args[0]:"";
			String value = (args!=null&&args.length>=2)?args[1]:"";
			ProducerConfig config = new ProducerConfig(props);
			Producer<String, String> producer = new Producer<String, String>(config);
			byte[] buffer = new byte[1024];
			while(System.in.read(buffer)>0) {
				String input = new String(buffer);
				String[] array = input.split("\t");
				if(array!=null && array.length==2) {
					key = array[0];
					value = array[1];
				} else if(array.length == 1) {
					value = array[0];
				}
				KeyedMessage<String, String> data = new KeyedMessage<String, String>("testLog",key,value);
				producer.send(data);
			}
			producer.close();
		}

}
