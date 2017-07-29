select
   fsav.id_flex_code segattr_id_flex_code,
   fsav.id_flex_num segattr_id_flex_num,
   fsav.application_column_name segattr_application_column_name,
   fsav.attribute_value ,
   fsav.segment_attribute_type 
from
   ${source_hive_schema_name}.applsys_fnd_segment_attribute_values fsav
