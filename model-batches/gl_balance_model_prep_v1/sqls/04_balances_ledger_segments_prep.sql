select
   gbls.*,
   fifs.application_id seg_application_id,
   fifs.id_flex_code seg_id_flex_code,
   fifs.id_flex_num seg_id_flex_num,
   fifs.application_column_name seg_application_column_name,
   fifs.segment_name,
   fifs.segment_num,
   fifs.flex_value_set_id seg_flex_value_set_id,
   fifs.default_type seg_default_type,
   fifs.default_value seg_default_value
from
   ${gl_balance_interim_cache} gbls,
   ${fnd_segment_cache} fifs
where
    gbls.chart_of_accounts_id = fifs.id_flex_num
    and gbls.application_column_name=fifs.application_column_name
    and fifs.id_flex_code='${fnd_id_flex_code}'
    and attribute_value='Y'
