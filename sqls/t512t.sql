create table bbs_tmgmt_hr_qa.t512t as
select
a.*
from
bbs_tmgmt_qa.t512t a,
(select molga,lgart,max(inserttime) minserttime from bbs_tmgmt_qa.t512t group by molga,lgart) b
where a.inserttime=b.minserttime and a.molga=b.molga and a.lgart=b.lgart
and a.sprsl='E'
