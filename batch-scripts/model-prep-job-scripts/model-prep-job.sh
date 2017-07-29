#!/bin/bash

#function to initialize the log directory and files
###########################################################################
function initLogFile()
###########################################################################
{
    if [ -z $job_log_directory ]; then
        echo "$(date): log directory was not configured...exiting"
        exit 1
    fi

    if [ ! -d $job_log_directory ]; then
        echo "$(date): log directory is not present, so creating it"
        mkdir -p $job_log_directory
    fi

    #creating the log file for current job execution
    touch $job_log_directory/$logFileName

    echo "check the $job_log_directory/$logFileName file to check about the log of this job..."
}
###########################################################################


#function to log the std out and std error messages into the log file
###########################################################################
function writeToLog()
###########################################################################
{
    echo "$(date) : $@" >> $job_log_directory/$logFileName 2>&1
}
###########################################################################


#function to log the stream of stdout and stderror messages into the log file
###########################################################################
function writeStreamToLog()
###########################################################################
{
    while read logLine; do
        echo "$(date) : $logLine" >> $job_log_directory/$logFileName 2>&1
    done
}
###########################################################################


#function to check the error code of the method to decide the exit status
###########################################################################
function checkExit()
###########################################################################
{
    if [ $? -ne 0 ]; then
        writeToLog "ERROR: job execution was failed...please check the log file.."
        exit 1
    fi
}
###########################################################################


#function to do the precheck of the arguments of the workflow
###########################################################################
function checkSparkWorkflowArguments()
###########################################################################
{
    if [ -z $job_spark_workflow_libpath ]; then
        writeToLog "ERROR: spark workflow lib was not configured...exiting"
        exit 1
    fi

    if [ -z $job_spark_dynamicAllocation_initialExecutors ] || [ -z $job_spark_dynamicAllocation_minExecutors ] || [ -z $job_spark_dynamicAllocation_maxExecutors ]; then
        writeToLog "ERROR: spark workflow execution resources were not configured...exiting"
        exit 1
    fi

    if [ -z $job_spark_job_queue_name ]; then
        writeToLog "ERROR: job queue name was not configured ...exiting"
        exit 1
    fi
}
###########################################################################


#function to execute the spark workflow application
###########################################################################
function executeSparkWorkflow()
###########################################################################
{
    writeToLog "executing the spark workflow job as configured in $workflowJobPath/workflow.xml"
 
#    spark-submit --class com.hashmap.mas.sparkflow.dispatch.WorkflowStepOrchestrator  --conf spark.shuffle.service.enabled=true  --conf spark.dynamicAllocation.enabled=true  --conf spark.dynamicAllocation.initialExecutors=${job_spark_dynamicAllocation_initialExecutors}  --conf spark.dynamicAllocation.minExecutors=${job_spark_dynamicAllocation_minExecutors}  --conf spark.dynamicAllocation.maxExecutors=${job_spark_dynamicAllocation_maxExecutors}  --master yarn  --deploy-mode client  --queue ${job_spark_job_queue_name}  ${job_spark_workflow_libpath} ${workflowJobPath}/workflow.xml  >> $job_log_directory/$logFileName 2>> $job_log_directory/$logFileName

spark-submit --class com.hashmap.mas.sparkflow.dispatch.WorkflowStepOrchestrator --driver-memory 6g --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=3 --conf spark.dynamicAllocation.minExecutors=2 --conf spark.dynamicAllocation.maxExecutors=40 --conf spark.driver.maxResultSize=5g --executor-memory 32g --executor-cores 8 --conf spark.yarn.executor.memoryOverhead=20000 --conf spark.sql.crossJoin.enabled=true --conf spark.memory.fraction=0.8 --conf spark.memory.storageFraction=0.3 --conf spark.broadcast.blockSize=1024m --conf  spark.default.parallelism=200 --conf spark.sql.shuffle.partitions=400  --master yarn --deploy-mode client  --queue  ${job_spark_job_queue_name} ${job_spark_workflow_libpath} ${workflowJobPath}/workflow.xml  >> $job_log_directory/$logFileName 2>> $job_log_directory/$logFileName


    #2>&1 | writeStreamToLog 
}
###########################################################################


#function to verify the given workflow path and files
###########################################################################
function verifyWorkflowFiles()
###########################################################################
{
    if [ $# -eq 0 ]; then
        writeToLog "ERROR: no command line arguments were supplied...exiting..."
        exit 1    
    fi

    if [ ! -d $1 ]; then
        writeToLog "ERROR: no such directory with name $1 found...exiting..."
        exit 1
    fi

    if [ ! -f $1/workflow.xml ] || [ ! -f $1/init-context.properties ]; then
        writeToLog "ERROR: the mandatory workflow.xml/init-context.properties files not present at $1...exiting..."
        exit 1
    fi

    if [ ! -d $1/sqls ]; then
        writeToLog "ERROR: no such directory with name sqls found at $1...exiting..."
        exit 1
    fi

    readonly workflowJobPath="$1"
}
###########################################################################


###########################################################################
#  MAIN PROGREAM BEGINS HERE
###########################################################################

    # recording the job start time
    dataLoadStartTime=$(date +%Y_%m_%d_%H%M%S)
    startTimeLog=`date +%s`   

    # getting the script file name
    programFileName=`basename "$0"`
    scriptFilePath=$(dirname $0)
    configFilePath="$scriptFilePath/jobconfig.properties"
    logFileName="$programFileName-$dataLoadStartTime.log"

    #checking the availability of the jobconfig.properties
    if [ ! -f $configFilePath ]; then
        echo "$(date): jobconfig.properties file is not found..exiting.."
        exit 1
    else
        #sourcing from the config file
        source $configFilePath
        checkExit $?

        #initializing the log file
        initLogFile
        checkExit $?
    fi
    
    writeToLog "INFO : job start time is $dataLoadStartTime"
    
    writeToLog "INFO : job_spark_workflow_libpath = $job_spark_workflow_libpath"
    writeToLog "INFO : job_spark_dynamicAllocation_initialExecutors = $job_spark_dynamicAllocation_initialExecutors"
    writeToLog "INFO : job_spark_dynamicAllocation_minExecutors = $job_spark_dynamicAllocation_minExecutors"
    writeToLog "INFO : job_spark_dynamicAllocation_maxExecutors = $job_spark_dynamicAllocation_maxExecutors"
    writeToLog "INFO : job_spark_job_queue_name = $job_spark_job_queue_name" 

    #checking the required configuration aruguments of the spark workflow job
    checkSparkWorkflowArguments
    checkExit $?


    #verify the required files presense in the given input directory
    verifyWorkflowFiles "$@"
    checkExit $?

    #executing the spark workflow application with the arguments configured
    executeSparkWorkflow
    checkExit $? 

    writeToLog "INFO: Successfully executed the spark workflow batch job.."

    writeToLog "INFO: Time taken to complete the batch job execution is $((`date +%s`-startTimeLog))"
    exit 1


