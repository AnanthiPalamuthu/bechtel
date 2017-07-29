select
    gll.*,
    gb.*
from
    ${gl_balance_cache} gb,
    ${gl_ledger_cache} gll
where
    gb.ledger_id = gll.dim_ledger_id
