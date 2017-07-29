select
    gjl.je_header_id line_je_header_id ,
    gjl.je_line_num line_je_line_num ,
    gjl.ledger_id line_ledger_id ,
    gjl.code_combination_id line_code_combination_id ,
    gjl.period_name period_name ,
    gjl.effective_date line_effective_date ,
    gjl.status line_status ,
    gjl.creation_date line_creation_date ,
    gjl.entered_dr line_entered_dr ,
    gjl.entered_cr line_entered_cr ,
    gjl.accounted_dr line_accounted_dr ,
    gjl.accounted_cr line_accounted_cr ,
    gjl.description line_description ,
    gjl.reference_1 line_reference_1 ,
    gjl.reference_2 line_reference_2 ,
    gjl.reference_3 line_reference_3 ,
    gjl.reference_4 line_reference_4 ,
    gjl.reference_5 line_reference_5 ,
    gjl.attribute1 line_attribute1 ,
    gjl.attribute2 line_attribute2 ,
    gjl.attribute3 line_attribute3 ,
    gjl.attribute4 line_attribute4 ,
    gjl.attribute5 line_attribute5 ,
    gjl.attribute6 line_attribute6 ,
    gjl.attribute7 line_attribute7 ,
    gjl.attribute8 line_attribute8 ,
    gjl.attribute9 line_attribute9 ,
    gjl.attribute10 line_attribute10 ,
    gjl.invoice_date line_invoice_date ,
    gjl.gl_sl_link_id line_gl_sl_link_id ,
    gjl.gl_sl_link_table line_gl_sl_link_table
from
    ${source_hive_schema_name}.gl_je_lines gjl
where
    unix_timestamp(${load.period.column.name},'${load.period.time.format}') > unix_timestamp(add_months(from_unixtime(unix_timestamp('${load.period.last.fetchtime}','${load.period.time.format}'),"yyyy-MM-dd hh:mm:ss"), ${load.period.reload.months}))
--    and  unix_timestamp(period_name,'MMM-yy') < unix_timestamp('JUL-16','MMM-yy')
