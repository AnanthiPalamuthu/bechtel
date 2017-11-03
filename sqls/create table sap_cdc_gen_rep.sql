CREATE EXTERNAL TABLE IF NOT EXISTS `sap_cdc_gen_rep`(                         
  `schema` string,                                           
  `filename` string,                                         
  `filesize` int,                                            
  `tablename` string,                                        
  `opr_ind` string,                                          
  `noofrecords` int,                                         
  `generatedtime` bigint,
  `ftpdownloadtime` bigint)                                       
COMMENT 'SAP Data generation report'                         
ROW FORMAT DELIMITED                                         
  FIELDS TERMINATED BY ','                                   
STORED AS TEXTFILE
    location '/user/s-bbs-bdac/sap_ops/sap_cdc_gen_rep/';

