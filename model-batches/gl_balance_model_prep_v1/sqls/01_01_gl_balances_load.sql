select 
    gb.*
from
    ${source_hive_schema_name}.gl_balances_partitioned gb
where 
    unix_timestamp(${load.period.column.name},'${load.period.time.format}') > unix_timestamp(add_months(from_unixtime(unix_timestamp('${load.period.last.fetchtime}','${load.period.time.format}'),"yyyy-MM-dd hh:mm:ss"), ${load.period.reload.months}))
--    and  unix_timestamp(${load.period.column.name},'MMM-yy') < unix_timestamp('JUL-15','MMM-yy')
