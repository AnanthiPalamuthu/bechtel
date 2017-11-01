echo "Usage : executeBatch.sh <ftpsource> <schema>"
echo "./executeBatch /inf/P01/HR/out bbs_sap_pr"
SCHEMA=$2
FTP_SOURCE_FOLDER=$1
FS_DEST_DIR=/home/IAMERS/s-bbs-bdac/table_dump/
HDFS_DEST_DIR=/user/s-bbs-bdac/pr_dump/
INGEST_TIME=$(date  +%Y%m%d%H%M)
FTPUSER=ftphadoop
FTPPW=3Ozg!@n-45F4#g=vc3_l                        # Better load this from an encrypted file
FTPSERVER=10.220.160.50
FTP=/usr/bin/ftp                    # Path to binary

mkdir ${FS_DEST_DIR}/${INGEST_TIME}
cd ${FS_DEST_DIR}/${INGEST_TIME}

echo -n "Downloadding files via FTP $INGEST_TIME ... "
$FTP -n $FTPSERVER << END_SCRIPT
quote USER $FTPUSER
quote PASS  $FTPPW
cd ${FTP_SOURCE_FOLDER}
prompt
mget *
#mdelete *
bye
END_SCRIPT

echo "creating CDC folder in HDFS"
/home/IAMERS/s-bbs-bdac/table_dump/bin/cdc_to_batch_conv.sh ${FS_DEST_DIR}/${INGEST_TIME}

filesOfLargerSize=`find . -type f -size +100k`

echo "Files of streaming non-allowable by size"
hdfs dfs -mkdir /user/s-bbs-bdac/pr_dump/CDC/$INGEST_TIME
hdfs dfs -put *.json /user/s-bbs-bdac/pr_dump/CDC/$INGEST_TIME

echo "Uploaded the files to HDFS folder /user/s-bbs-bdac/pr_dump/CDC/$INGEST_TIME"

echo "Converting the summary reports"
/home/IAMERS/s-bbs-bdac/table_dump/bin/summ_rep_gen.sh ${FS_DEST_DIR} ${INGEST_TIME} ${SCHEMA}
echo "upload the generated report to SAP GEN table"


echo "Submit the job to ingest into hive" 
