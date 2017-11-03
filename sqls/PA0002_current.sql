-- negation logic
drop view bbs_sap_pr.v_pa0002_ins_upd;
CREATE VIEW bbs_sap_pr.v_pa0002_ins_upd AS
SELECT pernr, begda, endda, concat(pernr,begda, endda) pbe FROM bbs_sap_pr.pa0002 
WHERE opr_ind in ('INS','UPD');
drop view bbs_sap_pr.v_pa0002_del;
CREATE VIEW bbs_sap_pr.v_pa0002_del AS
SELECT pernr, begda, endda, concat(pernr,begda, endda) pbe FROM bbs_sap_pr.pa0002 
WHERE opr_ind in ('DEL');
drop table bbs_sap_mediate.pa0002;
create table bbs_sap_mediate.pa0002 stored as orc AS
select pa.* from bbs_sap_pr.pa0002 pa, 
(SELECT a.pernr, a.begda, a.endda
FROM bbs_sap_pr.v_pa0002_ins_upd A
LEFT OUTER JOIN bbs_sap_pr.v_pa0002_del B
ON (B.pbe = A.pbe)
WHERE B.pbe IS null) pv
where pa.pernr = pv.pernr
and pa.begda = pv.begda
and pa.endda = pv.endda;


-- latest of the pernr
drop table bbs_sap_mediate.pa0002_t;
create table bbs_sap_mediate.pa0002_t stored as ORC as 
select distinct * from (
	select pernr, begda, endda, inserttime, opr_ind, 
	rank() over ( partition by pernr order by inserttime desc, opr_ind desc) as rank 
	from bbs_sap_mediate.pa0002 where (unix_timestamp (ENDDA, 'yyyyMMdd') > FROM_UNIXTIME(UNIX_TIMESTAMP()) or endda='99991231') and opr_ind != 'DEL'
	) t where rank = 1;


--final of the current table

drop table bbs_sap_hr_pr.pa0002;
create table bbs_sap_hr_pr.pa0002 stored as orc as
select distinct p1.aedtm, p1.anred, p1.anzkd, p1.begda, p1.cname, p1.endda, p1.famdt, p1.famst, p1.flag1, p1.flag2, p1.flag3, p1.flag4, p1.fnamk
, p1.fnamr, p1.gbdat, p1.gbdep, p1.gbjhr, p1.gblnd, p1.gbmon, p1.gbort, p1.gbpas, p1.gbtag, p1.gesch, p1.grpvl, p1.histo, p1.inits, p1.itbld, p1.itxex
, p1.knznm, p1.konfe, p1.lnamk, p1.lnamr, p1.mandt, p1.midnm, p1.nabik, p1.nabir, p1.nach2, p1.nachn, p1.nacon, p1.name2, p1.namz2, p1.namzu, p1.nati2
, p1.nati3, p1.natio, p1.nchmc, p1.nickk, p1.nickr, p1.objps, p1.ordex, p1.perid, p1.permo, p1.pernr, p1.preas, p1.refex, p1.rese1, p1.rese2, p1.rufnm
, p1.seqnr, p1.sprps, p1.sprsl, p1.subty, p1.titel, p1.titl2, p1.uname, p1.vnamc, p1.vorna, p1.vors2, p1.vorsw
from bbs_sap_mediate.pa0002 p1, bbs_sap_mediate.pa0002_t p2
where p1.pernr=p2.pernr and p1.inserttime=p2.inserttime and p1.begda = p2.begda and p1.endda=p2.endda and p1.opr_ind = p2.opr_ind;
	