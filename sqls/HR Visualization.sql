-- 
Select * --STATV, TEXT1,SPRSL 
from T529U 
where STATV = 3 and SPRSL = 'E' AND statn=2;
select * from pa0001;

-- PA0000 view
drop view bbs_sap_hr_pr.PA0000_v;
create view bbs_sap_hr_pr.PA0000_v as
select p.pernr as pernr, p.pernr as Personnel_Number,p.BEGDA, p.ENDDA,t.TEXT1 as Emp_Status
from bbs_sap_hr_pr.pa0000 p, T529U t
where p.stat2 = 3 and p.endda='99991231'
and p.stat2 = t.statv
and t.STATV = 3 and t.SPRSL = 'E' AND t.statn=2;

drop table bbs_sap_hr_pr.t527x_curr;
create table bbs_sap_hr_pr.t527x_curr as
Select * from  T527X  t7
where t7.SPRSL='E' and (unix_timestamp (t7.ENDDA, 'yyyyMMdd') > FROM_UNIXTIME(UNIX_TIMESTAMP()) or t7.endda='99991231');

drop table bbs_sap_hr_pr.HRP1000_curr;
create table bbs_sap_hr_pr.HRP1000 as
Select *  from HRP1000 h where h.OTYPE in ('S','C') and h.ISTAT = '1' --and h.OBJID = PA0001-PLANS 
and h.LANGU ='E' and (unix_timestamp (h.ENDDA, 'yyyyMMdd') > FROM_UNIXTIME(UNIX_TIMESTAMP()) or h.endda='99991231') ;

Select  STEXT  from HRP1000 where OTYPE = ‘C’ and ISTAT = '1'  (unix_timestamp (h.ENDDA, 'yyyyMMdd') > FROM_UNIXTIME(UNIX_TIMESTAMP()) or h.endda='99991231')

-- PA0001 view
drop table bbs_sap_hr_pr.HRP9102_curr;
create table bbs_sap_hr_pr.HRP9102_curr as
Select * from HRP9102 where  PLVAR = '01' and OTYPE = 'O'
 and (unix_timestamp (ENDDA, 'yyyyMMdd') > FROM_UNIXTIME(UNIX_TIMESTAMP()) or endda='99991231') ;

drop view bbs_sap_pr.bus_grp;
create view bbs_sap_pr.bus_grp as
Select hrp.PBUSLN_CD as PBUSLN_CD, zg.PBUSLN as PBUSLN 
from HRP9102 hrp , ZPD_org_BUS_GRP zg
where  zg.pbusln_cd = hrp.PBUSLN_CD and hrp.PLVAR = '01' and hrp.OTYPE = 'O'
and (unix_timestamp (hrp.ENDDA, 'yyyyMMdd') > FROM_UNIXTIME(UNIX_TIMESTAMP()) or hrp.endda='99991231') 
;   

select count(1) from bbs_sap_hr_pr.PA0001_v1;

drop view bbs_sap_hr_pr.PA0001_v;
create view bbs_sap_hr_pr.PA0001_v as
select distinct p1.pernr as pernr, p1.pernr as Personnel_Number, t2.name1 as Personnel_Area, t3.btext as Personnel_SubArea, t4.PTEXT as Emp_Group, t5.ptext as Emp_SubGroup, 
t6.ATEXT as Payroll_Area, p1.KOSTL as Cost_Center, t7.ORGTX as Org_Unit, h1.STEXT as Position, h2.stext as Job, p1.SNAME as Employee_Name, t8.ORGTX as Base_Org_Unit
, h3.PBUSLN_CD as Business_Group_Code, bg.PBUSLN as Business_Group, t1.BUTXT as Company_Code
from bbs_sap_hr_pr.pa0001 p1 , T001 t1 , T500P t2, T001P t3, T501t t4, T503t t5, T549T t6, bbs_sap_hr_pr.T527X t7, bbs_sap_hr_pr.HRP1000 h1
, bbs_sap_hr_pr.HRP1000 h2, bbs_sap_hr_pr.T527X t8,bbs_sap_hr_pr.HRP9102_curr h3, bbs_sap_pr.bus_grp bg
where p1.BUKRS = t1.BUKRS and p1.endda='99991231' 
and t2.bukrs = p1.WERKS
and t3.BTRTL = p1.btrtl and t3.werks = p1.werks
and p1.PERSG = t4.PERSG and t4.sprsl='E'
and t5.PERSK = p1.PERSK 
and t6.ABKRS =p1.ABKRS and t6.SPRSL = 'E'
and t7.ORGEH = P1.ORGEH --and t7.SPRSL='E' and (unix_timestamp (t7.ENDDA, 'yyyyMMdd') > FROM_UNIXTIME(UNIX_TIMESTAMP()) or t7.endda='99991231');
and h1.OBJID = P1.PLANS and h1.OTYPE = 'S' -- and h.ISTAT = '1' and h.LANGU ='E' and (unix_timestamp (h.ENDDA, 'yyyyMMdd') > FROM_UNIXTIME(UNIX_TIMESTAMP()) or h.endda='99991231') ;
and h2.OBJID = p1.STELL and h2.OTYPE = 'C' -- and h.ISTAT = '1' and h.LANGU ='E' and (unix_timestamp (h.ENDDA, 'yyyyMMdd') > FROM_UNIXTIME(UNIX_TIMESTAMP()) or h.endda='99991231') ;
and t8.ORGEH = p1.ZZBORG
and h3.OBJID = p1.ZZBORG
and bg.PBUSLN_CD = h3.PBUSLN_CD
;

	
-- PA90005 view
drop view bbs_sap_hr_pr.PA0005_v; 
create view bbs_sap_hr_pr.PA0005_v as
select distinct p5.pernr as pernr, p5.ZBECREF,t9.LTEXT  from bbs_sap_hr_pr.pa9005 p5,  T500t t9
where  p5.MOLGA = t9.MOLGA and t9.spras='E'; 
select * from t500t;

select distinct * from  bbs_sap_hr_pr.PA0000_v p0,  bbs_sap_hr_pr.PA0001_v p1, bbs_sap_hr_pr.PA0005_v p5
where p0.pernr = p1.pernr
and p1.pernr = p5.pernr
;

-- PA0002 view  (has UPD operation)
select distinct pernr,GESCH,GBDAT from PA0002;
select pernr, count(1) from bbs_sap_hr_pr.pa0002
group by pernr 
having count(1) > 1;

-- final visualization

drop table bbs_sap_hr_pr.master_data_enhanced;
create table bbs_sap_hr_pr.master_data_enhanced 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS Textfile 
as
select distinct p0.pernr as pernr, p0.Personnel_Number,p0.BEGDA, p0.ENDDA,p0.Emp_Status,
p1.Personnel_Area, p1.Personnel_SubArea, p1.Emp_Group, p1.Emp_SubGroup, 
p1.Payroll_Area, p1.Cost_Center, p1.Org_Unit, p1.Position, p1.Job, p1.Employee_Name, p1.Base_Org_Unit, p1.Company_Code
, p5.ZBECREF,p5.LTEXT, p1.Business_Group, p2.GBDAT as Date_of_Birth, 2017 - cast(p2.gbdat as int)  as age, case when p2.gesch = '1' then 'Male' when p2.gesch = '2' then 'Female' else 'Unknown' end as Gender
 from  bbs_sap_hr_pr.PA0000_v p0,  bbs_sap_hr_pr.PA0001_v p1, bbs_sap_hr_pr.PA0005_v p5, bbs_sap_hr_pr.pa0002 p2
where p0.pernr = p1.pernr
and p1.pernr = p5.pernr
and p2.pernr=p1.pernr
;