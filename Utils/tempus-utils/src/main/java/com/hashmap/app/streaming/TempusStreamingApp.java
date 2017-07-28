package com.hashmap.app.streaming;

import com.hashmap.app.TempusApp;

/**
 * Rowan streaming application
 * 
 * @author Jon Anderson
 *
 */
public interface TempusStreamingApp extends TempusApp {

	/** the command line option specifying the batch duration */
	public static final String BATCH_DURATION_OPTION = "d";
	/** the command line option specifying whether to ignore  */
	public static final String CHECKPOINT_DIR_OPTION = "c";
	/** Batch Duration Property */
	public static final String BATCH_DURATION_PROPERTY = "batch.duration";
	
	/**
	 * @return the streamingAdapter
	 */
	public StreamingAdapter<?> getStreamingAdapter();

}
