/**
 * 
 */
package com.hashmap.app;

import java.io.Serializable;

/**
 * Base interface for classes that will be helping the aggregator
 * 
 * @author Jon Anderson
 *
 */
public interface TempusService extends Serializable {

	/**
	 * Initialize the service 
	 * 
	 * @param rowanApp
	 */
	public void init(TempusApp rowanApp) throws Exception;

}
