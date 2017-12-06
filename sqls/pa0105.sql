CREATE VIEW bbs_tmgmt_qa.v_pa0105_ins_upd AS
SELECT pernr, begda, endda, concat(pernr,begda, endda) pbe FROM bbs_tmgmt_qa.pa0105 
WHERE opr_ind in ('INS','UPD');

CREATE VIEW bbs_tmgmt_qa.v_pa0105_del AS
SELECT pernr, begda, endda, concat(pernr,begda, endda) pbe FROM bbs_tmgmt_qa.pa0105 
WHERE opr_ind in ('DEL');

CREATE view bbs_tmgmt_hr_qa.pa0105 AS 
select 
pa.aedtm, 
pa.begda, 
pa.endda, 
pa.flag1, 
pa.flag2, 
pa.flag3, 
pa.flag4, 
pa.grpvl, 
pa.histo, 
pa.itbld, 
pa.itxex, 
pa.land1, 
pa.mandt, 
pa.objps, 
pa.opr_ind, 
pa.ordex, 
pa.pernr, 
pa.preas, 
pa.refex, 
pa.rese1, 
pa.rese2, 
pa.seqnr, 
pa.sprps, 
pa.subty, 
pa.telefto, 
pa.tel_areacode, 
pa.tel_extens, 
pa.tel_number, 
pa.uname, 
pa.usrid, 
pa.usrid_long, 
pa.usrty, 
pa.zzccard_actdt, 
pa.zzccard_deldt, 
pa.zzccard_effdt, 
pa.zzccard_expdt, 
pa.zzccard_issdt, 
pa.zzccard_name, 
pa.zzccard_status, 
pa.zzccard_tysid, 
pa.zzcountry, 
pa.inserttime from bbs_tmgmt_qa.pa0105 pa, 
(SELECT a.pernr, a.begda, a.endda 
FROM bbs_tmgmt_qa.v_pa0105_ins_upd a LEFT OUTER JOIN bbs_tmgmt_qa.v_pa0105_del b on (a.pbe=b.pbe) where b.pbe is null) pv
where pa.pernr=pv.pernr and pa.begda=pv.begda and pa.endda=pv.endda
