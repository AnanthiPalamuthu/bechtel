select
    jhb.*,
    glc.*
from
    ${gl_header_interim_cache} jhb,
    ${gl_ledger_cache} glc
where
    jhb.head_ledger_id = glc.ledger_ledger_id
