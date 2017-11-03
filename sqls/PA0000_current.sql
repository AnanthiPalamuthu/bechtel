-- negation logic
CREATE VIEW bbs_sap_pr.v_pa0000_ins_upd AS
SELECT pernr, begda, endda, concat(pernr,begda, endda) pbe FROM bbs_sap_pr.pa0000 
WHERE opr_ind in ('INS','UPD');
CREATE VIEW bbs_sap_pr.v_pa0000_del AS
SELECT pernr, begda, endda, concat(pernr,begda, endda) pbe FROM bbs_sap_pr.pa0000 
WHERE opr_ind in ('DEL');
drop table bbs_sap_mediate.pa0000;
create table bbs_sap_mediate.pa0000 stored as orc AS
select pa.* from bbs_sap_pr.pa0000 pa, 
(SELECT a.pernr, a.begda, a.endda
FROM bbs_sap_pr.v_pa0000_ins_upd A
LEFT OUTER JOIN bbs_sap_pr.v_pa0000_del B
ON (B.pbe = A.pbe)
WHERE B.pbe IS null) pv
where pa.pernr = pv.pernr
and pa.begda = pv.begda
and pa.endda = pv.endda;


-- latest of the pernr
drop table bbs_sap_mediate.pa0000_t;
create table bbs_sap_mediate.pa0000_t stored as ORC as 
select distinct * from (
	select pernr, begda, endda, inserttime, opr_ind, 
	rank() over ( partition by pernr order by inserttime desc, opr_ind desc) as rank 
	from bbs_sap_mediate.pa0000 where (unix_timestamp (ENDDA, 'yyyyMMdd') > FROM_UNIXTIME(UNIX_TIMESTAMP()) or endda='99991231') and opr_ind != 'DEL'
	) t where rank = 1;


--final of the current table
select distinct p1.aedtm, p1.begda, p1.endda, p1.flag1, p1.flag2, p1.flag3, p1.flag4, p1.grpvl, p1.histo, p1.itbld, p1.itxex, p1.mandt, p1.massg, p1.massn, p1.objps, p1.ordex, p1.pernr, p1.preas,
 p1.refex, p1.rese1, p1.rese2, p1.seqnr, p1.sprps, p1.stat1, p1.stat2, p1.stat3, p1.subty, p1.uname
from bbs_sap_mediate.pa0000 p1;
drop table bbs_sap_hr_pr.pa0000;
create table bbs_sap_hr_pr.pa0000 stored as orc as
select distinct p1.aedtm, p1.begda, p1.endda, p1.flag1, p1.flag2, p1.flag3, p1.flag4, p1.grpvl, p1.histo, p1.itbld, p1.itxex, p1.mandt, p1.massg, p1.massn, p1.objps, p1.ordex, p1.pernr, p1.preas,
 p1.refex, p1.rese1, p1.rese2, p1.seqnr, p1.sprps, p1.stat1, p1.stat2, p1.stat3, p1.subty, p1.uname
from bbs_sap_mediate.pa0000 p1, bbs_sap_mediate.pa0000_t p2
where p1.pernr=p2.pernr and p1.inserttime=p2.inserttime and p1.begda = p2.begda and p1.endda=p2.endda and p1.opr_ind = p2.opr_ind;
	