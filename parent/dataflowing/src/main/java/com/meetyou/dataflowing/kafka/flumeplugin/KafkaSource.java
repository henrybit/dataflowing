package com.meetyou.dataflowing.kafka.flumeplugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.Iterator;

/**
 * use this class to collect datas from kafka to flume.
 * <ul> 
 * <li><span style="font-weight:bold;">required: </span>The ‘kafka.topic’ string defines the Consumer listen to which topic of kafka cluster</li>
 * <li><span style="font-weight:bold;">required: </span>The ‘kafka.zookeeper.connect’ string identifies where to find once instance of Zookeeper in your cluster. Kafka uses ZooKeeper to store offsets of messages consumed for a specific topic and partition by this Consumer Group</li>
 * <li><span style="font-weight:bold;">required: </span>The ‘kafka.group.id’ string defines the Consumer Group this process is consuming on behalf of.</li>
 * <li><span style="font-weight:bold;">optional: </span>The ‘kafka.zookeeper.session.timeout.ms’ is how many milliseconds Kafka will wait for ZooKeeper to respond to a request (read or write) before giving up and continuing to consume messages.</li>
 * <li><span style="font-weight:bold;">optional: </span>The ‘kafka.zookeeper.sync.time.ms’ is the number of milliseconds a ZooKeeper ‘follower’ can be behind the master before an error occurs.</li>
 * <li><span style="font-weight:bold;">optional: </span>The ‘kafka.auto.commit.interval.ms’ setting is how often updates to the consumed offsets are written to ZooKeeper. Note that since the commit frequency is time based instead of # of messages consumed, if an error occurs between updates to ZooKeeper on restart you will get replayed messages.</li>
 * <li><span style="font-weight:bold;">optional: </span>The ‘kafka.stream.num’ setting is how much streams to be open.</li>
 * </ul>
 * @author henrybit
 * @version 1.0
 * @since 2014/09/25
 */
public class KafkaSource extends AbstractSource implements Configurable,
		PollableSource {
	
	private final static Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
	
	//consumer connect to the channel
	private ConsumerConnector consumerConnector;
	//Consumer的组ID
	private String GROUP_ID;
	//Consumer监听的Topic
	private String TOPIC;
	//Kafka的zookeeper连接
	private String ZOOKEEPER_CONNECT;
	//zookeeper的Session链接超时时长设置（单位：毫秒）
	private String ZOOKEEPER_SESSION_TIMEOUT_MS;
	//zookeeper的同步时间间隔（单位：毫秒）
	private String ZOOKEEPER_SYNC_TIME_MS;
	//更新Consumer的消息偏移信息到zookeeper上的时间间隔
	private String AUTO_COMMIT_INTERVAL_MS;
	//kafka的Consumer下的流链接数量
	private int STREAM_NUM;
	//Kafka的Consumer获取到的数据流列表
	private List<KafkaStream<byte[], byte[]>> streamList;
	private Iterator<MessageAndMetadata<byte[], byte[]>> iterator;
	
	//constant values
	private final static String CONSTANT_TOPIC = "kafka.topic";
	private final static String CONSTANT_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect";
	private final static String CONSTANT_GROUP_ID = "kafka.group.id";
	private final static String CONSTANT_ZOOKEEPER_SESSION_TIMEOUT = "kafka.zookeeper.session.timeout.ms";
	private final static String CONSTANT_ZOOKEEPER_SYNC_TIME = "kafka.zookeeper.sync.time.ms";
	private final static String CONSTANT_AUTO_COMMIT_INTERVAL = "kafka.auto.commit.interval.ms";
	private final static String CONSTANT_STREAM_NUM = "kafka.stream.num";
	
	@Override
	public synchronized void start() {
		// TODO Auto-generated method stub
		LOG.info("kafka source start.....");
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	    topicCountMap.put(TOPIC, STREAM_NUM);
	    LOG.info("process topic="+TOPIC+",num="+STREAM_NUM);
	    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
	    streamList = consumerMap.get(TOPIC);
	    LOG.info("Stream List size="+streamList.size());
	    
	    LOG.debug("KafkaSource process Stream list size="+streamList.size());
	    KafkaStream<byte[], byte[]> stream = streamList.get(0);
	    iterator = stream.toIterator();
		super.start();
	}

	@Override
	public synchronized void stop() {
		// TODO Auto-generated method stub
		LOG.info("kafka source stop.....");
		if(consumerConnector != null)
			consumerConnector.shutdown();
		super.stop();
	}

	public Status process() throws EventDeliveryException {
		LOG.info("start kafka source process.......");
		ArrayList<Event> eventList = new ArrayList<Event>();
		try {
			Event event;
			Map<String, String> headers;
			LOG.info("start get message from stream........");
			while(iterator.hasNext()) {
				MessageAndMetadata<byte[], byte[]> msgAndMetadata = iterator.next();
				event = new SimpleEvent();
				headers = new HashMap<String, String>();
				headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
				byte[] keybyte = msgAndMetadata.key();
				if(keybyte != null) {
					headers.put("key", new String(keybyte));
				}
				event.setBody(msgAndMetadata.message());
				event.setHeaders(headers);
				eventList.add(event);
				LOG.info("eventList add event:"+new String(event.getBody()));
				
				if(eventList.size() >= 100) {
					LOG.info("kafkaSource process "+eventList.size()+" messages");
					getChannelProcessor().processEventBatch(eventList);
					eventList = new ArrayList<Event>();
				}
			}
			return Status.READY;
		} catch (Exception e) {
			LOG.error("kafkaSource process occur exception"+e);
			return Status.BACKOFF;
		} finally {
			LOG.info("process end...........");
		}
	}

	public void configure(Context context) {
		LOG.info("Source configure start.........");
		TOPIC = context.getString(CONSTANT_TOPIC);
		ZOOKEEPER_CONNECT = context.getString(CONSTANT_ZOOKEEPER_CONNECT);
		GROUP_ID = context.getString(CONSTANT_GROUP_ID, String.valueOf(Math.random()));
		ZOOKEEPER_SESSION_TIMEOUT_MS = context.getString(CONSTANT_ZOOKEEPER_SESSION_TIMEOUT, "10000");
		ZOOKEEPER_SYNC_TIME_MS = context.getString(CONSTANT_ZOOKEEPER_SYNC_TIME, "400");
		AUTO_COMMIT_INTERVAL_MS = context.getString(CONSTANT_AUTO_COMMIT_INTERVAL, "1000");
		STREAM_NUM = context.getInteger(CONSTANT_STREAM_NUM, 1);
		LOG.info("Source processs configure topic="+TOPIC+",zookeeper="+ZOOKEEPER_CONNECT+",group.id="+GROUP_ID);
		
		Properties props = new Properties();
	    props.put("zookeeper.connect", ZOOKEEPER_CONNECT);
	    //props.put("zookeeper.connect", "zoo35:2181/kafka-0.8.1.1");
	    props.put("group.id", GROUP_ID);
	    //props.put("group.id", "1");
	    props.put("zookeeper.session.timeout.ms", ZOOKEEPER_SESSION_TIMEOUT_MS);
	    props.put("zookeeper.sync.time.ms", ZOOKEEPER_SYNC_TIME_MS);
	    props.put("auto.commit.interval.ms", AUTO_COMMIT_INTERVAL_MS);
	    ConsumerConfig consumerConfig = new ConsumerConfig(props);
	    consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
	    
	}

}