select
   ffvv.flex_value_id,
   ffvv.flex_value,
   ffvv.flex_value_set_id,
   ffvv.compiled_value_attributes,
   ffvv.value_category,
   ffvv.start_date_active,
   ffvv.end_date_active,
   ffvv.hierarchy_level,
   ffvv.flex_value_meaning,
   ffvv.description
from
   ${source_hive_schema_name}.apps_fnd_flex_values_vl ffvv
