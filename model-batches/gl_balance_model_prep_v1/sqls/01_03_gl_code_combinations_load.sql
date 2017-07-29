select
   gcc.code_combination_id code_code_combination_id,
   gcc.chart_of_accounts_id  code_chart_of_accounts_id,
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
   gcc.description  code_description,
   gcc.ledger_segment,
   gcc.ledger_type_code
from
   ${source_hive_schema_name}.gl_code_combinations gcc
