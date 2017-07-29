select
   fifs.application_id seg_application_id,
   fifs.id_flex_code seg_id_flex_code,
   fifs.id_flex_num seg_id_flex_num,
   fifs.application_column_name seg_application_column_name,
   fifs.segment_name,
   fifs.segment_num,
   fifs.flex_value_set_id,
   fifs.default_type,
   fifs.default_value
from
   ${source_hive_schema_name}.applsys_fnd_id_flex_segments fifs
