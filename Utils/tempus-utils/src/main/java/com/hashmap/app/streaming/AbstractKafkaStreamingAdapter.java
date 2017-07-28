/**
 * 
 */
package com.hashmap.app.streaming;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertyConverter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.OffsetRange;

import com.hashmap.app.TempusApp;

import kafka.common.TopicAndPartition;

/**
 * @author Jon Anderson
 * @param <K>
 *
 */
public abstract class AbstractKafkaStreamingAdapter<K> implements StreamingAdapter<K> {

	private static transient Logger log = Logger.getLogger(AbstractKafkaStreamingAdapter.class);
	
	public static final String KAFKA_TOPICS_PROPERTY = "kafka.topics";
    public static final String METADATA_BROKER_PROPERTY = "metadata.broker.list";
    public static final String ZOOKEEPER_CONNECT_PROPERTY = "zookeeper.connect";
    public static final String SECURITY_PROTOCOL_PROPERTY = "security.protocol";
    public static final String GROUP_ID_PROPERTY = "group.id";
    
	private static final long serialVersionUID = -1353348562327236768L;
	
	private Set<String> kafkaTopics = new HashSet<String>();
	private Map<String,String> kafkaParams = new HashMap<String,String>();
	private Properties properties = new Properties();
	
	/* (non-Javadoc)
	 * @see com.rowan.aggregator.StreamingAdapter#init(com.rowan.aggregator.StreamAggregator)
	 */
	@Override
	public void init(TempusApp rowanApp) throws Exception {
		if (rowanApp == null) {
			throw new IllegalArgumentException();
		}
		
		setProperties(rowanApp.getProperties());
		
		String s = getProperties().getProperty(METADATA_BROKER_PROPERTY);
		if (s == null) { 
			throw new ConfigurationException(METADATA_BROKER_PROPERTY + " not defined in config");
		}
		getKafkaParams().put(METADATA_BROKER_PROPERTY, s);

		s = getProperties().getProperty(ZOOKEEPER_CONNECT_PROPERTY);
		if (s == null) { 
			throw new ConfigurationException(ZOOKEEPER_CONNECT_PROPERTY + " not defined in config");
		}
		getKafkaParams().put(ZOOKEEPER_CONNECT_PROPERTY, s);

		s = rowanApp.getProperties().getProperty(KAFKA_TOPICS_PROPERTY);
		if (s == null) {
	    	throw new ConfigurationException("No kafka topics defined");
		}
    	@SuppressWarnings("unchecked")
		List<String> sa = PropertyConverter.split(s, ',');
    	if (sa.size() == 0) {
			throw new ConfigurationException(KAFKA_TOPICS_PROPERTY + " invalid");
    	}
    	
    	Set<String> topics = new HashSet<String>();
    	for (String topic : sa) { 
    		topics.add(topic);
    	}
    	setKafkaTopics(topics);

    	s = rowanApp.getProperties().getProperty(GROUP_ID_PROPERTY);
		if (s == null) {
			throw new ConfigurationException("No kafka topics defined");
		}
		getKafkaParams().put(GROUP_ID_PROPERTY, s);
		
	    s = getProperties().getProperty(SECURITY_PROTOCOL_PROPERTY);
	    if (s != null) {
	    	getKafkaParams().put(SECURITY_PROTOCOL_PROPERTY, s);
	    }
	}

	/* (non-Javadoc)
	 * @see com.rowan.aggregator.StreamingAdapter#createDStream(com.rowan.aggregator.StreamAggregator, org.apache.spark.streaming.api.java.JavaStreamingContext)
	 */
	@Override
	public abstract JavaPairDStream<String, K> createDStream(
			JavaStreamingContext javaStreamingContext) throws Exception;
	
	/**
	 * @return the kafkaTopics
	 */
	public Set<String> getKafkaTopics() {
		return kafkaTopics;
	}

	/**
	 * @param kafkaTopics the kafkaTopics to set
	 */
	public void setKafkaTopics(Set<String> kafkaTopics) {
		if (kafkaTopics == null) { 
			throw new IllegalArgumentException("kafkaTopics");
		}
		this.kafkaTopics = kafkaTopics;
	}

	/**
	 * @return the kafkaParams
	 */
	public Map<String, String> getKafkaParams() {
		return kafkaParams;
	}

	/**
	 * @param kafkaParams the kafkaParams to set
	 */
	public void setKafkaParams(Map<String, String> kafkaParams) {
		if (kafkaParams == null) { 
			throw new IllegalArgumentException("kafkaParams");
		}
		this.kafkaParams = kafkaParams;
	}

	/**
	 * @return the properties
	 */
	public Properties getProperties() {
		return properties;
	}

	/**
	 * @param properties the properties to set
	 */
	public void setProperties(Properties properties) {
		if (properties == null) {
			throw new IllegalArgumentException("properties");
		}
		this.properties = properties;
	}

	/* (non-Javadoc)
	 * @see com.rowan.aggregator.StreamingAdapter#openOffsetStore()
	 */
	@Override
	public StreamingAdapterOffsetStore openOffsetStore() {
		return new ZooKeeperOffsetStore();
	}

	/* (non-Javadoc)
	 * @see com.rowan.aggregator.StreamingAdapter#closeOffsetStore(com.rowan.aggregator.StreamingAdapterOffsetStore)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void closeOffsetStore(StreamingAdapterOffsetStore offsetStore) {
		if (offsetStore == null) {
			throw new IllegalArgumentException("offsetStore");
		}
		((ZooKeeperOffsetStore)offsetStore).close();
	}


	
	protected class ZooKeeperOffsetStore implements StreamingAdapterOffsetStore {

		private static final long serialVersionUID = 4016264413322464225L;

		private CuratorFramework client; 
		
		protected ZooKeeperOffsetStore() { 
			client = CuratorFrameworkFactory.newClient(
					getKafkaParams().get(ZOOKEEPER_CONNECT_PROPERTY), 
					new ExponentialBackoffRetry(500, 10));
			client.start();
			if (log.isDebugEnabled()) {
				log.debug("Opening zookeeper connection: " + 
						getKafkaParams().get(ZOOKEEPER_CONNECT_PROPERTY));
			}
		}
		
		/* (non-Javadoc)
		 * @see com.rowan.aggregator.StreamingAdapterOffsetStore#writeOffsetRanges(org.apache.spark.streaming.kafka.OffsetRange[])
		 */
		@Override
		public void writeOffsetRanges(OffsetRange[] offsetRanges) {
			if (offsetRanges != null) {
				
				for (OffsetRange offsetRange : offsetRanges) {
					
	//				byte[] ba = new byte[8];
	//				ByteBuffer.wrap(ba).putLong(offsetRange.fromOffset());
					byte[] ba = Long.toString(offsetRange.fromOffset()).getBytes();
					String path = createKafkaConsumerPath(offsetRange);
	
					if (log.isDebugEnabled()) {
						log.debug("Kafka Offset: " 
								+ path + " from "+ offsetRange.fromOffset() + " to " + 
								offsetRange.untilOffset());
					}
					
					try {
						if (client.checkExists().forPath(path) == null) {
							client.create().creatingParentsIfNeeded().forPath(path, ba);
						} else { 
							client.setData().forPath(path, ba);
						}
						
					} catch (Exception e) {
						log.error("Error updating offsets: ", e);
					}
				}
			}
		}

		public Map<TopicAndPartition, Long> readOffsetRanges() {

			StringBuilder sb = new StringBuilder();
			sb.append("/consumers/");
			sb.append(getKafkaParams().get(GROUP_ID_PROPERTY));
			sb.append("/offsets");
			
			String offsetsPath = sb.toString();
			try {
				List<String> topics = client.getChildren().forPath(offsetsPath);
				
				Map<TopicAndPartition, Long> offsetRanges = 
						new HashMap<TopicAndPartition,Long>();
				
				for (String topic : topics) {
					
					String topicPath = offsetsPath + "/" + topic; 
					
					List<String> partitions = client.getChildren().forPath(topicPath);
					for (String partition : partitions) {
						
						int p = Integer.parseInt(partition);
						TopicAndPartition tanp = new TopicAndPartition(topic, p);
						String partitionPath = topicPath + "/" + p; 
						
						byte[] val = client.getData().forPath(partitionPath);
						String sval = new String(val);
						Long lval = Long.parseLong(sval);
						
						offsetRanges.put(tanp, lval);
						
						if (log.isDebugEnabled()) {
							log.debug("Offset ranges read at: " + partitionPath + " = " + lval);
						}
					}
				}
				
				return offsetRanges;
				
			} catch (Exception e) {
				log.warn("Error reading offsets for: " + offsetsPath, e);
			}
			
			return null;
		}
		
		public String createKafkaConsumerPath(OffsetRange offsetRange) {
			if (offsetRange == null) {
				throw new IllegalArgumentException("offsetRange");
			}
			
			StringBuilder sb = new StringBuilder();
			sb.append("/consumers/");
			sb.append(getKafkaParams().get(GROUP_ID_PROPERTY));
			sb.append("/offsets/");
			sb.append(offsetRange.topic());
			sb.append("/");
			sb.append(offsetRange.partition());
			return sb.toString();
		}
		
		protected void close() {
			if (log.isDebugEnabled()) {
				log.debug("Closing zookeeper connection");
			}
			client.close();
		}
		
	}


	public static void main(String[] args) {
		
		AbstractKafkaStreamingAdapter<String> adapter = new AbstractKafkaStreamingAdapter<String>() {

			public JavaPairDStream<String, String> createDStream(
					JavaStreamingContext javaStreamingContext) throws Exception {
				return null;
			}
		};
		
		Map<String, String> params = new HashMap<String, String>();
		params.put(AbstractKafkaStreamingAdapter.GROUP_ID_PROPERTY, "stream-aggregator");
		params.put(AbstractKafkaStreamingAdapter.KAFKA_TOPICS_PROPERTY, "topic_mqtt_rowan_1");
		params.put(AbstractKafkaStreamingAdapter.METADATA_BROKER_PROPERTY, "atlhashed01.hashmap.net:6667");
		params.put(AbstractKafkaStreamingAdapter.ZOOKEEPER_CONNECT_PROPERTY, "atlhashdn01.hashmap.net:2181,atlhashdn02.hashmap.net:2181,atlhashdn03.hashmap.net:2181");

		adapter.setKafkaParams(params);
		
		AbstractKafkaStreamingAdapter.ZooKeeperOffsetStore offsetStore = 
				(AbstractKafkaStreamingAdapter.ZooKeeperOffsetStore)adapter.openOffsetStore();
		
		offsetStore.readOffsetRanges();
		adapter.closeOffsetStore(offsetStore);
	}
}
