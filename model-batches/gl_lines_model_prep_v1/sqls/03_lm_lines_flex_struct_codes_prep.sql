select
    glc.*,
    fscic.*
from
    ${flex_structure_codes_interim_cache} fscic,
    ${gl_lines_cache} glc
where
    glc.line_code_combination_id = fscic.code_code_combination_id
