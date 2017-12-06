create table bbs_tmgmt_hr_qa.catsdb as 
select distinct 
pernr ,
counter,
workdate,
awart,
lgart,
unit,
ersda ,
opr_ind,
erstm ,
ernam,
laeda,
laetm,
aenam,
status, 
refcounter,
catshours,
prakn,
beguz,
enduz,
ltxa1,
catsquantity,
zzjcd,
zzacd,
zzssc,
zzorg,
zzloc,
zzrbo,
zzent,
zzact,
zzcst,
zzreleaseddt,
zzreleasedtm,
zzreleasedby,
apdat, 
zzapprovedtm,
apnam,
zzreason,
zzreasontx,
zzcat2,
zzpmflag
from
bbs_tmgmt_qa.catsdb a,
(select counter bcounter,max(concat(laeda,laetm)) mladatm,max(inserttime) minserttime from bbs_tmgmt_qa.catsdb group  by counter) b
where 
a.inserttime=b.minserttime and a.counter=b.bcounter
and a.opr_ind !='DEL' and
case when(a.opr_ind=='INS' or a.opr_ind=='UPD') then concat(a.laeda,a.laetm)=b.mladatm end 
