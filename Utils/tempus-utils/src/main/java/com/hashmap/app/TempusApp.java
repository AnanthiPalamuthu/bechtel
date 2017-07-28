/**
 * 
 */
package com.hashmap.app;

import java.io.Serializable;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author Jon Anderson
 *
 */
public interface TempusApp extends Serializable {
	
	/** the command line option specifying the properties URL */
	String PROPERTIES_OPTION = "p";
	/** the command line option specifying the app name */
	String APP_NAME_OPTION = "n";

	/**
	 * @return the properties
	 */
	public Properties getProperties();

	/**
	 * @return the commandLine
	 */
	public CommandLine getCommandLine();
	
	/**
	 * 
	 * @return the spark context
	 */
	public JavaSparkContext getJavaSparkContext();

}
