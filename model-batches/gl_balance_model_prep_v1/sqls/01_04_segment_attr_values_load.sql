select
   fsav.id_flex_num,
   fsav.id_flex_code,
   fsav.application_column_name,
   fsav.attribute_value ,
   fsav.segment_attribute_type 
from
   ${source_hive_schema_name}.applsys_fnd_segment_attribute_values fsav
