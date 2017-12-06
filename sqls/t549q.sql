create table bbs_tmgmt_hr_qa.t549q as 
select
a.*
from
bbs_tmgmt_qa.t549q a,
(select permo,pabrj,pabrp,max(inserttime) minserttime from  bbs_tmgmt_qa.t549q group by permo,pabrj,pabrp) b
where a.inserttime=b.minserttime and a.permo=b.permo and a.pabrj=b.pabrj and a.pabrp=b.pabrp
