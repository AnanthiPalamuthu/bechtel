create table bbs_tmgmt_hr_qa.t001 as 
select
a.*
from
bbs_tmgmt_qa.t001 a,
(select bukrs,max(inserttime) minserttime from  bbs_tmgmt_qa.t001 group by bukrs) b
where a.inserttime=b.minserttime and a.bukrs=b.bukrs
