create table bbs_tmgmt_hr_qa.t549s as 
select
a.*
from
bbs_tmgmt_qa.t549s a,
(select permo,molga,max(inserttime) minserttime from  bbs_tmgmt_qa.t549s group by permo,molga) b
where a.inserttime=b.minserttime and a.permo=b.permo and a.molga=b.molga
