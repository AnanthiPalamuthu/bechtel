
echo "looking at the ${1} location to find the recent fetch time..."

echo "the value read from the above location = `hadoop fs -cat ${1}/part-r-0000*`"

latestPeriodValues=`hadoop fs -cat ${1}/part-r-0000*`

periodValuesArray=(${latestPeriodValues//,/ })

lastFetchPeriodConfigured="${periodValuesArray[0]}"

lastFetchPeriodAdjusted="${periodValuesArray[1]}"

lastFetchJobRecentPeriod="${periodValuesArray[2]}"

echo "period=${lastFetchPeriodConfigured}:${lastFetchPeriodAdjusted}:${lastFetchJobRecentPeriod}"

sed -i.bak 's/^\(load.period.last.fetchtime=\).*/\1'"$lastFetchJobRecentPeriod"'/' ${2}/../init-context.properties

cp ${2}/13_lm_prepare_gl_lines_flat_table.sql.template ${2}/13_lm_prepare_gl_lines_flat_table.sql

sed -i "s|START_PERIOD|${lastFetchPeriodAdjusted}|g" ${2}/13_lm_prepare_gl_lines_flat_table.sql

sed -i "s|END_PERIOD|${lastFetchJobRecentPeriod}|g" ${2}/13_lm_prepare_gl_lines_flat_table.sql

echo "updated the given file with the recent value fetched."
