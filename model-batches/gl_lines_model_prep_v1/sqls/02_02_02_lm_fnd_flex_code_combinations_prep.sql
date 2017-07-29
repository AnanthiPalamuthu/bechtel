select
    ffsic.*,
    ccc.*
from
    ${fnd_flex_structure_interim_cache} ffsic,
    ${code_combination_cache} ccc
where
    ccc.code_chart_of_accounts_id = ffsic.flex_structure_id_flex_num
