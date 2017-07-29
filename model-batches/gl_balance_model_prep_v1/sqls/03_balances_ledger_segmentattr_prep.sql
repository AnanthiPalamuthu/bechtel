select
   gbl.*,
   fsav.application_column_name,
   fsav.attribute_value ,
   fsav.segment_attribute_type 
from
   ${gl_balance_interim_cache} gbl,
   ${gl_segment_attr_cache} fsav
where
    gbl.chart_of_accounts_id = fsav.id_flex_num
    and id_flex_code='${fnd_id_flex_code}'
    and segment_attribute_type='${segment_attr_type}'
    and attribute_value='Y'
