
echo "looking at the ${1} location to find the recent fetch time..."

echo "the value read from the above location = `hadoop fs -cat ${1}/part-r-0000*`"



latestPeriodValues=`hadoop fs -cat ${1}/part-r-0000*`



periodValuesArray=(${latestPeriodValues//,/ })

lastFetchPeriodConfigured="${periodValuesArray[0]}"

lastFetchPeriodAdjusted="${periodValuesArray[1]}"

lastFetchJobRecentPeriod="${periodValuesArray[2]}"

echo "period=${lastFetchPeriodConfigured}:${lastFetchPeriodAdjusted}:${lastFetchJobRecentPeriod}"



sed -i.bak 's/^\(load.period.last.fetchtime=\).*/\1'"$lastFetchJobRecentPeriod"'/' ${2}/init-context.properties

echo "updated the init-context.properties file with the recent time value..."



cp ${2}/sqls/09_prepare_gl_balance_flat_table.sql.template ${2}/sqls/09_prepare_gl_balance_flat_table.sql

sed -i "s|START_PERIOD|${lastFetchPeriodAdjusted}|g" ${2}/sqls/09_prepare_gl_balance_flat_table.sql

sed -i "s|END_PERIOD|${lastFetchJobRecentPeriod}|g" ${2}/sqls/09_prepare_gl_balance_flat_table.sql

echo "updated the sql files with the relevant time values..."



cp ${2}/druid-files/druid-ingestion-conf.properties.template ${2}/druid-files/druid-ingestion-conf.properties

sed -i "s|TBD_START_MONTH|${lastFetchPeriodAdjusted}|g" ${2}/druid-files/druid-ingestion-conf.properties

sed -i "s|TBD_END_MONTH|${lastFetchJobRecentPeriod}|g" ${2}/druid-files/druid-ingestion-conf.properties

sed -i "s|TBD_SRC_PATH|${3}|g" ${2}/druid-files/druid-ingestion-conf.properties

echo "updated the druid ingestion properties file with the time boundary values..."
