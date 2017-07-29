select 
    concat( "${load.period.last.fetchtime}",
            ",",
            from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp('${load.period.last.fetchtime}','${load.period.time.format}'),"yyyy-MM-dd"),${load.period.reload.months}),"yyyy-MM-dd"),'${load.period.time.format}'), 
            ",",  
            from_unixtime(max(unix_timestamp(period_name,'MMM-yy')),'MMM-yy')) 
from 
    ${gl_lines_interim_cache}
