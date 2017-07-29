select
   glic.*,
   fsav.*
from
   ${gl_line_interim_cache} glic,
   ${flex_segment_attr_cache} fsav
where
    glic.code_chart_of_accounts_id = fsav.segattr_id_flex_num
    and fsav.segattr_id_flex_code = '${fnd_id_flex_code}'
    and fsav.segment_attribute_type = '${segment_attr_type}'
    and fsav.attribute_value = 'Y'
