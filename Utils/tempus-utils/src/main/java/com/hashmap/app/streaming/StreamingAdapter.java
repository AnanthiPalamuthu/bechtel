/**
 * 
 */
package com.hashmap.app.streaming;


import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.hashmap.app.TempusService;

/**
 * Classes responsible for providing DStreams to the 
 * 
 * @author Jon Anderson
 */
public interface StreamingAdapter<V> extends TempusService {
	
	
	/** 
	 * Method called to create a new DStream
	 * @param javaStreamingContext the context
	 * 
	 * @return not null
	 * 
	 * @throws Exception
	 */
	public JavaPairDStream<String, V> createDStream(
			JavaStreamingContext javaStreamingContext) throws Exception;
	
	/**
	 * 
	 * @return
	 */
	public StreamingAdapterOffsetStore openOffsetStore();

	/**
	 * 
	 * @param offsetStore
	 */
	public void closeOffsetStore(StreamingAdapterOffsetStore offsetStore);

}
