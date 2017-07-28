/**
 * 
 */
package com.hashmap.app.streaming;

import java.io.Serializable;

import org.apache.spark.streaming.kafka.OffsetRange;

/**
 * @author Jon Anderson
 *
 */
public interface StreamingAdapterOffsetStore extends Serializable {
	
	/**
	 * 
	 * @param offsetRanges
	 */
	public void writeOffsetRanges(OffsetRange[] offsetRanges);
	

}
