select
   gbl.*,
   gcc.code_chart_of_accounts_id,
   gcc.account_type  ,
   gcc.segment1  ,
   gcc.segment2  ,
   gcc.segment3  ,
   gcc.segment4  ,
   gcc.segment5  ,
   gcc.segment6  ,
   gcc.segment7  ,
   gcc.segment8  ,
   gcc.segment9  ,
   gcc.segment10  ,
   gcc.code_description,
   gcc.ledger_segment,
   gcc.ledger_type_code
from
   ${gl_balance_interim_cache} gbl,
   ${code_combinations_cache} gcc
where
    gbl.code_combination_id = gcc.code_code_combination_id
    and gbl.chart_of_accounts_id = gcc.code_chart_of_accounts_id
