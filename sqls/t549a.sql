create table bbs_tmgmt_hr_qa.t549a as 
select
a.*
from
bbs_tmgmt_qa.t549a a,
(select abkrs,max(inserttime) minserttime from  bbs_tmgmt_qa.t549a group by abkrs) b
where a.inserttime=b.minserttime and a.abkrs=b.abkrs
