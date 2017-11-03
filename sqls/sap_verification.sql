set tez.queue.name=spark;

select * from HRP1002 where OBJID in ("50681223", "50681224", "50681275") order by inserttime desc;
select * from HRP1007 where OBJID in ("50681223", "50681224", "50681275") order by inserttime desc;
select * from HRP1008 where OBJID in ("50681223", "50681224", "50681275") order by inserttime desc;

select * from pa0000 where pernr in ("00053273","00053273") order by inserttime desc;
select * from pa0000 pa1 where pa1.pernr in (
select pa2.pernr from pa0000 pa2 where pa2.opr_ind='DEL') order by pa1.pernr, pa1.inserttime  desc;

select * from pa0001 pa1 where pa1.pernr in (
select pa2.pernr from pa0001 pa2 where pa2.opr_ind in ('UPD', 'DEL')) order by pa1.pernr, pa1.inserttime  desc;
select * from pa0006 pa where pa.opr_ind='UPD';
select pa.inserttime, pa.opr_ind, count(1) from pa0006 pa group by pa.inserttime , pa.opr_ind ;
select inserttime, opr_ind, count(1) from ZPA_PSS_WD_USAGE pa group by pa.inserttime , pa.opr_ind ;
select filename, tablename, noofrecords from bbs_sap_pr.ingestion_summary where filename like 'hdfs://OSWEGOPROD/user/s-bbs-bdac/pr_dump/20170920204312/%' order by tablename;
select distinct inserttype, inserttime from bbs_sap_pr.ingestion_summary order by inserttime desc;
select * from bbs_sap_ops.ingestion_summary where schema="bbs_sap_py_qa" order by inserttime desc;

select *, from_unixtime(CAST(inserttime/1000 as BIGINT), 'yyyy-MM-dd HH:mm'), inserttime 
from ZPA_INTF_DATETO order by inserttime desc ;

select filename, inserttime , from_unixtime(CAST(inserttime/1000 as BIGINT), 'yyyy-MM-dd HH:mm') from dead_messages;

select *, inserttime, from_unixtime(CAST(inserttime/1000 as BIGINT), 'yyyy-MM-dd HH:mm') insttime from bbs_sap_qa.pa0021
where pernr = '00141560'
order by inserttime desc;


create table bbs_sap_pr.pa0006_ana stored as orc as select * from bbs_sap_qa.pa0006;

show tables;
select distinct inserttime , from_unixtime(CAST(inserttime/1000 as BIGINT), 'yyyy-MM-dd HH:mm') from pa0008;
select filename, inserttype, tablename, from_unixtime(CAST(inserttime/1000 as BIGINT), 'yyyy-MM-dd HH:mm'), inserttime, noofrecords 
from ingestion_summary where tablename in ('T5UTB') order by inserttime desc ;

select * from catsdb where ersda = '20170921' limit 10;

select * from PA0105 where pernr = '00622933' aND SUBTY ='CELL' and aedtm != '00000000' order by inserttime desc limit 1000;

select distinct aedtm from pa0003 where opr_ind = 'DEL' limit 100;
select *, max(aedtm)  from pa0105 group by *
show tables;

drop table ingestion_summary_0919_4;

select a.* from pa0105 a , pa0105 b where b.pernr = a.pernr and a.aedtm = ;

desc extended bnka;

select count(1) from bbs_sap_qa.pa0105; --452488

select count(1) from bbs_sap_qa.pa0002; --197966

select * from bbs_sap_qa.pa0105 where pernr = "00053079" order by inserttime desc;

select * from bbs_sap_qa.pa0002 where pernr = "00053080" order by inserttime desc;

select  a.* from bbs_sap_qa.pa0002 a, bbs_sap_qa.pa0002 b 
where a.pernr = "00053080" and a.opr_ind in ('INS', 'UPD')
and a.pernr=b.pernr
and b.opr_ind = 'DEL'
and (a.begda != b.begda and  a.endda != b.endda) ;

select a.* from pa0002 a, 
(select pernr, max(inserttime) inserttime from bbs_sap_qa.pa0002 where opr_ind in ('INS','UPD')  group by pernr) b
where a.pernr=b.pernr and a.inserttime = b.inserttime
and a.pernr = "00053080" 
union 
select a.* from pa0002 a, 
(select pernr, max(inserttime) inserttime from bbs_sap_qa.pa0002 where opr_ind in ('INS')  group by pernr) b
where a.pernr=b.pernr and a.inserttime = b.inserttime
and a.pernr = "00053080";


select a.* from pa0105 a, 
(select pernr, max(inserttime) inserttime from bbs_sap_qa.pa0105 where opr_ind in ('UPD')  group by pernr) b
where a.pernr=b.pernr and a.inserttime = b.inserttime
and a.pernr = "00053079" 
union
select a.* from pa0105 a, 
(select pernr, max(inserttime) inserttime from bbs_sap_qa.pa0105 where opr_ind in ('INS')  group by pernr) b
where a.pernr=b.pernr and a.inserttime = b.inserttime
and a.pernr = "00053079" ;

select * from bbs_sap_qa.pa0016 where pernr = "00053077" order by inserttime desc;
select * from bbs_sap_qa.pa0022 where pernr = "00053079" order by inserttime desc;
select * from bbs_sap_qa.pa0022 where pernr = "00053078" order by inserttime desc;
select * from bbs_sap_qa.pa0105 where pernr = "00141107" and subty='CELL' order by inserttime desc;
select * from bbs_sap_qa.pa0006 where pernr = "00138803" AND SUBTY='4' order by inserttime desc;

select distinct a.mandt, a.opr_ind, a.pernr, a.subty, a.objps, a.begda, a.endda, a.seqnr, a.aedtm, a.uname from bbs_sap_pr.PA0001 a, 
(select pernr, max(inserttime) inserttime from bbs_sap_pr.PA0001 where opr_ind in ('INS')  group by pernr) b
where a.pernr=b.pernr and a.inserttime = b.inserttime
and a.pernr = "00050232" ;
select distinct a.mandt, a.opr_ind, a.pernr, a.subty, a.objps, a.begda, a.endda, a.seqnr, a.aedtm, a.uname, a.inserttime from bbs_sap_pr.PA0001 a where a.pernr = '00050232' order by  a.inserttime desc;

select c.mandt, c.pernr, c.begda, c.endda from 
(select a.mandt, a.pernr, a.begda, a.endda from bbs_sap_qa.v_pa0105_ins_upd a 
minus
select b.mandt, b.pernr, b.begda, b.endda from bbs_sap_qa.v_pa0105_del b ) c;

select a.mandt, a.pernr, a.begda, a.endda from bbs_sap_qa.pa0105 a where a.opr_ind in ('INS', 'UPD');

CREATE VIEW bbs_sap_qa.v_pa0105_ins_upd AS
SELECT pernr, begda, endda, concat(pernr,begda, endda) pbe FROM bbs_sap_qa.pa0105 
WHERE opr_ind in ('INS','UPD');
CREATE VIEW bbs_sap_qa.v_pa0105_del AS
SELECT pernr, begda, endda, concat(pernr,begda, endda) pbe FROM bbs_sap_qa.pa0105 
WHERE opr_ind in ('DEL');

select * from bbs_sap_qa.pa0105; 

select a.pernr, a.begda, a.endda from bbs_sap_qa.v_pa0105_ins_upd a ,bbs_sap_qa.v_pa0105_ins_upd b
where a.pbe != b.pbe
and a.pernr = "00053079" ;

select pa.* from bbs_sap_qa.pa0105 pa, 
(SELECT a.pernr, a.begda, a.endda
FROM bbs_sap_qa.v_pa0105_ins_upd A
LEFT OUTER JOIN bbs_sap_qa.v_pa0105_del B
ON (B.pbe = A.pbe)
WHERE B.pbe IS null) pv
where pa.pernr = pv.pernr
and pa.begda = pv.begda
and pa.endda = pv.endda;
--and pa.pernr = "00053079";

create view bbs_sap_qa.pa0105_v AS
select pa.* from bbs_sap_qa.pa0105 pa, 
(SELECT a.pernr, a.begda, a.endda
FROM bbs_sap_qa.v_pa0105_ins_upd A
LEFT OUTER JOIN bbs_sap_qa.v_pa0105_del B
ON (B.pbe = A.pbe)
WHERE B.pbe IS null) pv
where pa.pernr = pv.pernr
and pa.begda = pv.begda
and pa.endda = pv.endda;

select * from bbs_sap_qa.pa0105_v where ;

drop view bbs_sap_qa.v_pa0105_ins_upd;
drop view bbs_sap_qa.v_pa0105_del;

SELECT * FROM bbs_sap_qa.pa0001
WHERE opr_ind in ('INS', 'UPD');

-- PA0002 views 

CREATE VIEW bbs_sap_qa.v_pa0002_ins_upd AS
SELECT pernr, begda, endda, concat(pernr,begda, endda) pbe FROM bbs_sap_qa.pa0105 
WHERE opr_ind in ('INS','UPD');
CREATE VIEW bbs_sap_qa.v_pa0002_del AS
SELECT pernr, begda, endda, concat(pernr,begda, endda) pbe FROM bbs_sap_qa.pa0105 
WHERE opr_ind in ('DEL');
create view bbs_sap_qa.v_pa0002 AS
select pa.* from bbs_sap_qa.pa0002 pa, 
(SELECT a.pernr, a.begda, a.endda
FROM bbs_sap_qa.v_pa0002_ins_upd A
LEFT OUTER JOIN bbs_sap_qa.v_pa0002_del B
ON (B.pbe = A.pbe)
WHERE B.pbe IS null) pv
where pa.pernr = pv.pernr
and pa.begda = pv.begda
and pa.endda = pv.endda;

select pernr, opr_ind, aedtm, begda, endda, inserttime, from_unixtime(CAST(inserttime/1000 as BIGINT), 'yyyy-MM-dd HH:mm') instime  from pa0001  where pernr = '00161519' order by inserttime desc;

select * from bbs_sap_ops.sap_gen_rep where inserttime = 1507227308000;


select rep.filename, rep.tablename, rep.opr_ind from bbs_sap_ops.sap_gen_rep rep
left outer join bbs_sap_ops.ingestion_summary ings
on (rep.tablename=ings.tablename and rep.opr_ind =ings.opr_ind and (ings.inserttime >= 1507227308000))
where rep.inserttime = 1507227308000 
and ings.tablename is null;

select pernr, subty,begda, endda, aedtm, uname, from_unixtime(CAST(inserttime/1000 as BIGINT), 'yyyy-MM-dd HH:mm') instime,* from bbs_sap_qa.pa0021 where pernr ="00141560" order by inserttime desc;

select from_unixtime(CAST(inserttime/1000 as BIGINT), 'yyyy-MM-dd HH:mm') instime,* from bbs_sap_qa.pa0006 where pernr ="00050316" order by inserttime desc;


# pa0002 : Time constraint T

select pa.* from bbs_sap_qa.pa0002 pa, 
(SELECT a.pernr, a.begda, a.endda
FROM bbs_sap_qa.v_pa0002_ins_upd A
LEFT OUTER JOIN bbs_sap_qa.v_pa0002_del B
ON (B.pbe = A.pbe)
WHERE B.pbe IS null) pv
where pa.pernr = pv.pernr
and pa.begda = pv.begda
and pa.endda = pv.endda
and pa.pernr = "00053079";


CREATE VIEW bbs_sap_qa.v_pa0006_ins_upd AS
SELECT pernr, begda, endda, concat(pernr,begda, endda) pbe FROM bbs_sap_qa.pa0006 
WHERE opr_ind in ('INS','UPD');
CREATE VIEW bbs_sap_qa.v_pa0006_del AS
SELECT pernr, begda, endda, concat(pernr,begda, endda) pbe FROM bbs_sap_qa.pa0006 
WHERE opr_ind in ('DEL');
create view bbs_sap_qa.pa0006_v AS
select pa.* from bbs_sap_qa.pa0006 pa, 
(SELECT a.pernr, a.begda, a.endda
FROM bbs_sap_qa.v_pa0006_ins_upd A
LEFT OUTER JOIN bbs_sap_qa.v_pa0006_del B
ON (B.pbe = A.pbe)
WHERE B.pbe IS null) pv
where pa.pernr = pv.pernr
and pa.begda = pv.begda
and pa.endda = pv.endda;

select distinct pernr, subty, begda, endda, opr_ind, locat, from_unixtime(CAST(inserttime/1000 as BIGINT), 'yyyy-MM-dd HH:mm') instime, inserttime from bbs_sap_qa.pa0006 where pernr = "00050316" order by inserttime desc ;

--time constraint T
select *, from_unixtime(CAST(inserttime/1000 as BIGINT), 'yyyy-MM-dd HH:mm') instime, inserttime  from bbs_sap_qa.pa0105 where pernr = "00053079" order by inserttime desc;
    
select *, from_unixtime(CAST(inserttime/1000 as BIGINT), 'yyyy-MM-dd HH:mm') instime from bbs_sap_ops.ingestion_summary  order by inserttime desc ;

select *, from_unixtime(CAST(inserttime/1000 as BIGINT), 'yyyy-MM-dd HH:mm') instime from ZTJCD_tmp where inserttime > 1508065221000 order by inserttime desc ; 
 
 select * from bbs_sap_ops.sap_gen_rep; 
 select * from bbs_sap_ops.ingestion_summary limit 10;

