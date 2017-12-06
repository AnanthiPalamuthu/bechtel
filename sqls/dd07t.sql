create table bbs_tmgmt_hr_qa.dd07t as
select
a.*
from
bbs_tmgmt_qa.dd07t a,
(select domname,domvalue_l,max(inserttime) minserttime from bbs_tmgmt_qa.dd07t group by domname,domvalue_l) b
where a.inserttime=b.minserttime and a.domname=b.domname and a.domvalue_l=b.domvalue_l and ddlanguage='E'
and as4local='A'