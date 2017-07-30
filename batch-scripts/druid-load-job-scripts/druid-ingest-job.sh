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


#function to do the precheck of the arguments of the druid ingestion
###########################################################################
function checkDruidIngestionArguments()
###########################################################################
{
    if [ ! -f $druidScriptsPath/druid-datasource-ingest-job.json.template ]; then
        writeToLog "ERROR: druid datasource ingestion json was not found...exiting"
        exit 1
    fi

    if [ -z $druid_datasource_name ] || [ -z $druid_ingestion_startmonth ] || [ -z $druid_ingestion_endmonth ] || [ -z $druid_ingestion_hdfspath ]; then
        writeToLog "ERROR: All/Any of the Druid data source ingestion arguments were not set were not configured...exiting"
        exit 1
    fi
}
###########################################################################


#function to do the alter the druid ingestion json with given parameters values
###########################################################################
function alterDruidIngestionJson()
###########################################################################
{

    cp ${druidScriptsPath}/druid-datasource-ingest-job.json.template ${druidScriptsPath}/druid-datasource-ingest-job.json

    sed -i "s|TBD_DATASOURCE_NAME|${druid_datasource_name}|g" ${druidScriptsPath}/druid-datasource-ingest-job.json

    sed -i "s|TBD_START_DATE|${druid_ingestion_startmonth}|g" ${druidScriptsPath}/druid-datasource-ingest-job.json

    sed -i "s|TBD_END_DATE|${druid_ingestion_endmonth}|g" ${druidScriptsPath}/druid-datasource-ingest-job.json 

    sed -i "s|TBD_HDFS_FILEPATH|${druid_ingestion_hdfspath}|g" ${druidScriptsPath}/druid-datasource-ingest-job.json

    writeToLog "updated the druid datasource ingestion json with the parameter given..."    
}
###########################################################################


#function to execute the spark workflow application
###########################################################################
function executeDruidIngestionJson()
###########################################################################
{
    writeToLog "executing the druid datasource ingestion json from ${druidScriptsPath}/druid-datasource-ingest-job.json"

    readonly ingestJobID=`curl -X 'POST' -H 'Content-Type:application/json' -d @${druidScriptsPath}/druid-datasource-ingest-job.json localhost:8099/druid/indexer/v1/task | grep "task" | python -c "import sys, json; print json.load(sys.stdin)['task']"`
 
    writeToLog "Druid datasource ingestion job id=${ingestJobID}"

}
###########################################################################


#function to hold execution until the becomes into SUCCESS/FAILED status
###########################################################################
function checkDruidIngestionJobStatus()
###########################################################################
{
    writeToLog "starting an infinite loop to check until the ${ingestJobID} comes to SUCCESS/FAILED status"

    ingestJobStatus="RUNNING"

    while [[ ("${ingestJobStatus}" != "SUCCESS"  && ( "${ingestJobStatus}" = "FAILED" || "${ingestJobStatus}" = "RUNNING" )) || ("${ingestJobStatus}" != "FAILED"  && ( "${ingestJobStatus}" = "RUNNING" || "${ingestJobStatus}" = "RUNNING" )) ]]; do

        ingestJobStatus="`curl -v -X 'GET' http://localhost:8099/druid/indexer/v1/task/${ingestJobID}/status | grep "task" | python -c "import sys, json; print json.load(sys.stdin)['status']['status']"`"

        writeToLog "current status of the job is ${ingestJobStatus}"
        
        sleep 2s
 
    done
	
    writeToLog "Druid datasource ingestion job ${ingestJobID} was completed with status ${ingestJobStatus}"

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
    druidScriptsPath="${1}"
    configFilePath="${druidScriptsPath}/druid-ingestion-conf.properties"
    logFileName="$programFileName-$dataLoadStartTime.log"

    #checking the availability of the jobconfig.properties
    if [ ! -f $configFilePath ]; then
        echo "$(date): druid-ingestion-conf.properties file is not found..exiting.."
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
    
    writeToLog "INFO : druid_datasource_name = ${druid_datasource_name}"
    writeToLog "INFO : druid_ingestion_startmonth = ${druid_ingestion_startmonth}"
    writeToLog "INFO : druid_ingestion_endmonth = ${druid_ingestion_endmonth}"
    writeToLog "INFO : druid_ingestion_hdfspath = ${druid_ingestion_hdfspath}"


    #checking the required configuration aruguments of the druid ingestion job
    checkDruidIngestionArguments
    checkExit $?

    #updating the druid datasource ingestion json with the given values
    alterDruidIngestionJson
    checkExit $?

    #executing the Druid datasource ingestion json
    executeDruidIngestionJson
    checkExit $? 

    #holding until the ingestion job comes to SUCCES/FAILED status
    checkDruidIngestionJobStatus
    checkExit $?

    writeToLog "INFO: Successfully executed the druid datasource ingestion batch job.."

    writeToLog "INFO: Time taken to complete the batch job execution is $((`date +%s`-startTimeLog))"
	
    exit 0s