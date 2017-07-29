select
    gblc.*,
    ffvv.flex_value_id,
    ffvv.flex_value,
    ffvv.compiled_value_attributes,
    ffvv.value_category,
    ffvv.start_date_active,
    ffvv.end_date_active,
    ffvv.hierarchy_level,
    ffvv.flex_value_meaning,
    ffvv.description flex_value_description
from
   ${gl_balance_interim_cache} gblc,
   ${fnd_flex_values_cache} ffvv
where
    gblc.seg_flex_value_set_id = ffvv.flex_value_set_id
    and ffvv.flex_value = (case when(gblc.application_column_name='SEGMENT1') then gblc.segment1
                               when(gblc.application_column_name='SEGMENT2') then gblc.segment2
                               when(gblc.application_column_name='SEGMENT3') then gblc.segment3
                               when(gblc.application_column_name='SEGMENT4') then gblc.segment4
                               when(gblc.application_column_name='SEGMENT5') then gblc.segment5
                               when(gblc.application_column_name='SEGMENT6') then gblc.segment6
                               else gblc.segment2
                          end)
