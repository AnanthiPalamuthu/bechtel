select
    glcc.code_combination_id code_code_combination_id ,
    glcc.chart_of_accounts_id code_chart_of_accounts_id ,
    glcc.account_type code_account_type ,
    glcc.segment1 code_segment1 ,
    glcc.segment2 code_segment2 ,
    glcc.segment3 code_segment3 ,
    glcc.segment4 code_segment4 ,
    glcc.segment5 code_segment5 ,
    glcc.segment6 code_segment6 ,
    glcc.segment7 code_segment7 ,
    glcc.segment8 code_segment8 ,
    glcc.segment9 code_segment9 ,
    glcc.segment10 code_segment10 ,
    glcc.description code_description ,
    glcc.start_date_active code_start_date_active ,
    glcc.end_date_active code_end_date_active ,
    glcc.ledger_segment code_ledger_segment ,
    glcc.ledger_type_code code_ledger_type_code ,
    glcc.alternate_code_combination_id code_alternate_code_combination_id
from
    ${source_hive_schema_name}.gl_code_combinations glcc
