echo "Usage rep_gen.sh ${FS_DEST_DIR} ${INGEST_TIME} <filename> <schema>" 
fname=`basename $1`
schema=$2
#echo "fname=$fname"
DATETIME=$(echo $fname | grep -Po '(?=)\d{14}.csv' | sed 's/.csv//g')
rm sap_gen_rep-$DATETIME.txt

while read p; do
 # echo $p
 p1=$(echo $p | sed 's/,,/,INS,/g')
 TABLENAME=$(echo $p1 | grep -Po '^\w+,' | sed 's/,//g')
 OPR=$(echo $p1 | grep -Po '(?=),\w{3},' | sed 's/,//g')
 RECORDCOUNT=$(echo $p1 | grep -Po '(?=),\d+' | sed 's/,//g')  
 filename=$TABLENAME-$OPR$DATETIME.json
 filesize=$(stat -c%s "$filename")
 echo "$schema,$filename,$filesize,$TABLENAME,$OPR,$RECORDCOUNT,$DATETIME" >> sap_gen_rep-$DATETIME.txt

done < $1
