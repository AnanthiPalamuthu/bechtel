select
    glic.*,
    fsc.*
from
    ${gl_line_interim_cache} glic,
    ${flex_segment_cache} fsc
where
    glic.code_chart_of_accounts_id = fsc.seg_id_flex_num
    and glic.segattr_application_column_name=fsc.seg_application_column_name
    and fsc.seg_id_flex_code='${fnd_id_flex_code}'
    and glic.attribute_value='Y'
