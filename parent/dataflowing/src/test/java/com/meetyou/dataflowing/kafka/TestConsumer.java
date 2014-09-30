package com.meetyou.dataflowing.kafka;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

public class TestConsumer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		  Properties props = new Properties();
	      props.put("zookeeper.connect", "zoo35:2181/kafka-0.8.1.1");
	      props.put("group.id", "2");
	      props.put("zookeeper.session.timeout.ms", "400");
	      props.put("zookeeper.sync.time.ms", "200");
	      props.put("auto.commit.interval.ms", "1000");
	      ConsumerConfig consumerConfig = new ConsumerConfig(props);
	      ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
	      Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	      topicCountMap.put("testLog", new Integer(1));
	      Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
	      List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("testLog");
	      System.out.println(streams.size());
	      KafkaStream<byte[], byte[]> stream = streams.get(0);
	      for(MessageAndMetadata<byte[], byte[]> msgAndMetadata: stream) {
	    	  byte[] keybyte = msgAndMetadata.key();
	    	  if(keybyte != null) {
	    		  String key = new String(msgAndMetadata.key());
	    		  System.out.println("key="+key);
	    	  }
	    	  System.out.println("value="+new String(msgAndMetadata.message()));
	      }
	}

	
	public static void test1(String[] args) {
		// TODO Auto-generated method stub
		Properties props = new Properties();
        
        props.put("zookeeper.connect", "zoo35:2181");
        props.put("group.id", "1");
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        //props.put("kafka.topic", "testLog");
        
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        System.out.println("init config finish...");
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
        System.out.println("create connector finish...");
        Map<String, Integer> topicsMap = new HashMap<String, Integer>();
        topicsMap.put("testLog", 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicsMap);
        System.out.println("create stream finish....");
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("testLog");
        System.out.println("get topic's messages...."+streams.size());
        System.out.println(streams);
        KafkaStream<byte[], byte[]> stream = streams.get(0);
//        for (KafkaStream<byte[], byte[]> stream : streams) {
        	System.out.println("get message..."+stream.size());
        	for(MessageAndMetadata msgAndMetadata : stream) {
        		System.out.println("topic:"+msgAndMetadata.topic());
        		Message message = (Message) msgAndMetadata.message();
        		ByteBuffer buffer = message.payload();
        		byte[] bytes = new byte[message.payloadSize()];
        		buffer.get(bytes);
        		String temp = new String(bytes);
        		System.out.println("message recevie:"+temp);
        	}
//        }
        System.out.println("over");
        
        while(true) {
        	continue;
        }
	}
	
	 public static void test(String[] args) {  
	        // specify some consumer properties  
	        Properties props = new Properties();  
	        props.put("zookeeper.connect", "192.168.10.35:2181");  
	        props.put("zookeeper.session.timeout.ms", "1000000");  
	        props.put("group.id", "test_group");  
	  
	        // Create the connection to the cluster  
	        ConsumerConfig consumerConfig = new ConsumerConfig(props);  
	        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);  
	  
	        HashMap<String, Integer> map = new HashMap<String, Integer>();  
	        map.put("testLog", 1);  
	        Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams =  
	                consumerConnector.createMessageStreams(map);  
	        List<KafkaStream<byte[], byte[]>> streams = topicMessageStreams.get("testLog");  
	  
	        for (final KafkaStream<byte[], byte[]> stream : streams) {  
	            for (MessageAndMetadata msgAndMetadata : stream) {  
	                // process message (msgAndMetadata.message())  
	                System.out.println("topic: " + msgAndMetadata.topic());  
	                Message message = (Message) msgAndMetadata.message();  
	                ByteBuffer buffer = message.payload();  
	                byte[] bytes = new byte[message.payloadSize()];  
	                buffer.get(bytes);  
	                String tmp = new String(bytes);  
	                System.out.println("message content: " + tmp);  
	            }  
	        }
	    }  

}
