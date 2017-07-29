select
    gp.period_set_name period_period_set_name ,
    gp.period_name period_period_name ,
    gp.start_date period_start_date ,
    gp.end_date period_end_date ,
    gp.year_start_date period_year_start_date ,
    gp.quarter_start_date period_quarter_start_date ,
    gp.period_type period_period_type ,
    gp.period_year period_period_year ,
    gp.period_num period_period_num ,
    gp.quarter_num period_quarter_num ,
    gp.entered_period_name period_entered_period_name ,
    gp.creation_date period_creation_date ,
    gp.description period_description 
from
    ${source_hive_schema_name}.gl_periods gp
