-- negation logic
CREATE VIEW bbs_tmgmt_qa.v_pa0007_ins_upd AS
SELECT pernr, begda, endda, concat(pernr,begda, endda) pbe FROM bbs_tmgmt_qa.pa0007 
WHERE opr_ind in ('INS','UPD');
CREATE VIEW bbs_tmgmt_qa.v_pa0007_del AS
SELECT pernr, begda, endda, concat(pernr,begda, endda) pbe FROM bbs_tmgmt_qa.pa0007 
WHERE opr_ind in ('DEL');
drop table bbs_tmgmt_mediate.pa0007;
create table bbs_tmgmt_mediate.pa0007 stored as orc AS
select pa.* from bbs_tmgmt_qa.pa0007 pa, 
(SELECT a.pernr, a.begda, a.endda
FROM bbs_tmgmt_qa.v_pa0007_ins_upd A
LEFT OUTER JOIN bbs_tmgmt_qa.v_pa0007_del B
ON (B.pbe = A.pbe)
WHERE B.pbe IS null) pv
where pa.pernr = pv.pernr
and pa.begda = pv.begda
and pa.endda = pv.endda;


-- latest of the pernr
drop table bbs_tmgmt_mediate.pa0007_t;
create table bbs_tmgmt_mediate.pa0007_t stored as ORC as 
select distinct * from (
select pernr, begda, endda, inserttime, opr_ind, 
rank() over ( partition by pernr order by inserttime desc, opr_ind desc) as rank 
from bbs_tmgmt_mediate.pa0007 where (unix_timestamp (ENDDA, 'yyyyMMdd') > FROM_UNIXTIME(UNIX_TIMESTAMP()) or endda='99991231') and opr_ind != 'DEL'
) t where rank = 1;


--final of the current table

drop table bbs_tmgmt_hr_qa.pa0007;
create table bbs_tmgmt_hr_qa.pa0007 stored as orc as
select distinct p1.abkrs, p1.aedtm, p1.ansvh, p1.begda, p1.btrtl, p1.budget_pd, p1.bukrs, p1.ename, p1.endda, p1.fistl, p1.fkber, p1.flag1
, p1.flag2, p1.flag3, p1.flag4, p1.geber, p1.grant_nbr, p1.grpvl, p1.gsber, p1.histo, p1.itbld, p1.itxex, p1.juper, p1.kokrs, p1.kostl, p1.mandt
, p1.mstbr, p1.objps, p1.ordex, p1.orgeh, p1.otype, p1.pernr, p1.persg, p1.persk, p1.plans, p1.preas, p1.refex, p1.rese1, p1.rese2, p1.sacha
, p1.sachp, p1.sachz, p1.sbmod, p1.seqnr, p1.sgmnt, p1.sname, p1.sprps, p1.stell, p1.subty, p1.uname, p1.vdsk1, p1.werks, p1.zzborg, p1.zzscaoc
from bbs_tmgmt_mediate.pa0007 p1, bbs_tmgmt_mediate.pa0007_t p2
where p1.pernr=p2.pernr and p1.inserttime=p2.inserttime and p1.begda = p2.begda and p1.endda=p2.endda and p1.opr_ind = p2.opr_ind;