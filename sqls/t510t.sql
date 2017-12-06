create table bbs_tmgmt_hr_qa.t510t as 
select
a.*
from 
bbs_tmgmt_qa.t510t a,
( select prakn,max(inserttime) minserttime from bbs_tmgmt_qa.t510t group by prakn)b
where a.inserttime=b.minserttime and a.prakn=b.prakn