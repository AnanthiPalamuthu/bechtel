search_dir=$1
echo "Converting the streaming message to message for batch on files in folder $search_dir"
BASEDIR=$(dirname "$search_dir")
echo $BASEDIR
for entry in "$search_dir"/*.json

do
  
  fname=`basename $entry`
  sed -i -- 's/},{/}\n{/g' $entry
  sed -i -- 's/}\]}/}/g' $entry
  echo "entry:$entry $fname"
  TABLENAME=$(echo $fname | awk -F'[-.]'  '{ print $1 }') 
  OPR=$(echo $fname | grep -Po '(?=)-\w{3}' | sed 's/-//g')
  DATETIME=$(echo $fname | grep -Po '(?=)\d{14}')

  TABLESTR="{\"table\":\"$TABLENAME\",\"rows\":\[{"
  echo $TABLESTR
  sed -i -- 's/{"table":"'"$TABLENAME"'","rows":\[{/{/g' $entry

  sed -i -- 's/{"table":"'"$TABLENAME"'","TIMESTAMP":"'"$DATETIME"'","rows":\[{/{/g' $entry
done
echo "Converted the streaming message to message for batch on files in folder $search_dir"
