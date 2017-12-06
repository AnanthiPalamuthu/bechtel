-- negation logic
drop view bbs_tmgmt_qa.v_pa0008_ins_upd;
CREATE VIEW bbs_tmgmt_qa.v_pa0008_ins_upd AS
SELECT pernr, begda, endda, concat(pernr,begda, endda) pbe FROM bbs_tmgmt_qa.pa0008 
WHERE opr_ind in ('INS','UPD');
drop view bbs_tmgmt_qa.v_pa0008_del;
CREATE VIEW bbs_tmgmt_qa.v_pa0008_del AS
SELECT pernr, begda, endda, concat(pernr,begda, endda) pbe FROM bbs_tmgmt_qa.pa0008 
WHERE opr_ind in ('DEL');
drop table bbs_tmgmt_mediate.pa0008;
create table bbs_tmgmt_mediate.pa0008 stored as orc AS
select pa.* from bbs_tmgmt_qa.pa0008 pa, 
(SELECT a.pernr, a.begda, a.endda
FROM bbs_tmgmt_qa.v_pa0008_ins_upd A
LEFT OUTER JOIN bbs_tmgmt_qa.v_pa0008_del B
ON (B.pbe = A.pbe)
WHERE B.pbe IS null) pv
where pa.pernr = pv.pernr
and pa.begda = pv.begda
and pa.endda = pv.endda;


-- latest of the pernr
drop table bbs_tmgmt_mediate.pa0008_t;
create table bbs_tmgmt_mediate.pa0008_t stored as ORC as 
select distinct * from (
select pernr, begda, endda, inserttime, opr_ind, 
rank() over ( partition by pernr order by inserttime desc, opr_ind desc) as rank 
from bbs_tmgmt_mediate.pa0008 where (unix_timestamp (ENDDA, 'yyyyMMdd') > FROM_UNIXTIME(UNIX_TIMESTAMP()) or endda='99991231') and opr_ind != 'DEL'
) t where rank = 1;


--final of the current table

drop table bbs_tmgmt_hr_qa.pa0008;
create table bbs_tmgmt_hr_qa.pa0008 stored as orc as
select distinct p1.*
from bbs_tmgmt_mediate.pa0008 p1, bbs_tmgmt_mediate.pa0008_t p2
where p1.pernr=p2.pernr and p1.inserttime=p2.inserttime and p1.begda = p2.begda and p1.endda=p2.endda and p1.opr_ind = p2.opr_ind;