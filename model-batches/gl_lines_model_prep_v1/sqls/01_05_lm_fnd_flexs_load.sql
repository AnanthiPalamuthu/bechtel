select
    fif.application_id flex_application_id ,
    fif.id_flex_code flex_id_flex_code ,
    fif.id_flex_name flex_id_flex_name ,
    fif.table_application_id flex_table_application_id ,
    fif.application_table_name flex_application_table_name ,
    fif.allow_id_valuesets flex_allow_id_valuesets ,
    fif.unique_id_column_name flex_unique_id_column_name ,
    fif.description flex_description ,
    fif.application_table_type flex_application_table_type 
from
    ${source_hive_schema_name}.applsys_fnd_id_flexs fif
