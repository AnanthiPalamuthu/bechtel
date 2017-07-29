select
    ffc.*,
    ffsc.*
from
    ${fnd_flex_cache} ffc,
    ${fnd_flex_structure_cache} ffsc
where
    ffc.flex_application_id = ffsc.flex_structure_application_id
    and ffc.flex_id_flex_code='${fnd_id_flex_code}'
    and ffsc.flex_structure_id_flex_code = '${fnd_id_flex_code}'
    and ffsc.flex_structure_language = '${language}'
