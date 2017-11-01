echo "Usage summ_rep_gen.sh <base_dir> <download_timestamp> <schema>"
echo "Converting the SAP provided report to comparison required report for $1/$2"

search_dir=$1/$2
ftpdownloadtime=$2
schema=$3
cd $search_dir
BASEDIR=$(dirname "$search_dir")
echo $BASEDIR
for entry in "$search_dir"/INGESTION_COUNT*.csv

do
	fname=`basename $entry`
	echo " working on $fname"
	SAPDATETIME=$(echo $fname | grep -Po '(?=)\d{14}.csv' | sed 's/.csv//g')
	rm $search_dir/sap_gen_rep-$SAPDATETIME.csv

	while read p; do
	 # echo $p
	 p1=$(echo $p | sed 's/,,/,INS,/g')
	 TABLENAME=$(echo $p1 | grep -Po '^\w+,' | sed 's/,//g')
	 OPR=$(echo $p1 | grep -Po '(?=),\w{3},' | sed 's/,//g')
	 RECORDCOUNT=$(echo $p1 | grep -Po '(?=),\d+' | sed 's/,//g')
	 filename=$TABLENAME-$OPR$SAPDATETIME.json
	 
	 filesize=$(stat -c%s "$search_dir/$filename")
	 echo "$schema,$filename,$filesize,$TABLENAME,$OPR,$RECORDCOUNT,$SAPDATETIME,$ftpdownloadtime" >> $search_dir/sap_gen_rep-$SAPDATETIME.csv
	done < $entry
     echo "converted $fname generated $search_dir/sap_gen_rep-$SAPDATETIME.csv"
     echo "uploading to hive externally managed table sap_gen_rep"
     # hdfs dfs -put $search_dir/sap_gen_rep-$SAPDATETIME.csv /user/s-bbs-bdac/sap_ops/sap_cdc_gen_rep/

done
echo "Converted the SAP provided report to comparison required report for $1/$2"
