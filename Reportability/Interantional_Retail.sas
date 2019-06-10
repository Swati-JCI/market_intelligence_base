
%let remedy_date=042219;
%let traffic_date=050519;
%let run_date=051019;
%let start_date=mdy(02,04,2018);
%let end_date=mdy(05,04,2019);


libname out"Q:\Project\02.DataCollection\02.02.ConvertedData";
libname raw "Q:\Project\02.DataCollection\02.02.ConvertedData";

 data WORK.remedy_info_&remedy_date.    ;
    %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
    infile "Q:\Users\Pokkuluri117659\02.Data collection\01.Raw data\remedy_info_&remedy_date..csv" delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
       informat SITE_ID best32. ;
       informat SITE_NAME $100. ;
       informat SITE_TYPE $20. ;
       informat CHAIN_ID best32. ;
       informat CHAIN_NAME $100. ;
	   informat ORGANIZATION_ID best32. ;
       informat ORGANIZATION_NAME $100. ;
       informat CITY $50. ;
       informat COUNTRY $20. ;
       informat STATE_PROV $4. ;
       informat ZIP $15. ;
       informat SMS_BILL_IND $3. ;
       informat EXTERNAL_SITE_ID $50. ;
       format SITE_ID best20. ;
       format SITE_NAME $50. ;
       format SITE_TYPE $20. ;
       format CHAIN_ID best32. ;
       format CHAIN_NAME $50. ;
       format ORGANIZATION_ID best20. ;
       format ORGANIZATION_NAME $100. ;
       format CITY $50. ;
       format COUNTRY $20. ;
       format STATE_PROV $4. ;
       format ZIP $20. ;
       format SMS_BILL_IND $3. ;
       format EXTERNAL_SITE_ID $50. ;
    input
                SITE_ID
                SITE_NAME $
                SITE_TYPE $
                CHAIN_ID
                CHAIN_NAME $
                ORGANIZATION_ID
                ORGANIZATION_NAME $
                CITY $
                COUNTRY $
                STATE_PROV $
                ZIP $
                SMS_BILL_IND $
                EXTERNAL_SITE_ID $
    ;
    if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
    run;


proc sql threads feedback;
create table country_sites as 
select site_id, chain_id,site_type,chain_name, city, state_prov, zip, country, sms_bill_ind, 
organization_id,organization_name, external_site_id
from remedy_info_&remedy_date.
where site_id^=.;
quit;

data France_Regions_Postcodes                ;
    %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
    infile 'Q:\Users\Priyabrat118879\02.Data Collection\02.01.RawData\Index_France_Regions&Post_codes.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
	   informat Region $24. ;
	   informat Region_details $24. ;
       informat Postal_codes_start_by $2. ;
	   format Region $24. ;
       format Region_details $24. ;
       format Postal_codes_start_by $2. ;
    input
				Region $
			    Region_details $
                Postal_codes_start_by
    ;
    if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
    run;

data WORK.combined_brands                         ;
   %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
   infile 'Q:\Users\Sayoni93677\02.DataCollection\02.01.RawData\CN_CombinedAPR.csv' delimiter
= ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
      informat Org_id best32. ;
      informat org_name $44. ;
      informat new_org_id best32. ;
      informat new_org_name $47. ;
      format Org_id best12. ;
      format org_name $44. ;
      format new_org_id best12. ;
      format new_org_name $47. ;
   input
               Org_id
               org_name $
               new_org_id
               new_org_name $
   ;
   if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
   run;

data WORK.NRF_MONTH_NUM                           ;
    %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
    infile 'Q:\Users\Srihita114499\02.DataCollection\02.01.RawData\month_order.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
       informat NRF_Month $11. ;
       informat NRF_Month_Num best32. ;
       format NRF_Month $11. ;
       format NRF_Month_Num best12. ;
    input
                NRF_Month $
                NRF_Month_Num
    ;
    if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
    run;  
 
data country_sites;
set country_sites;
if country="UK" then country = "GB";
run;

proc sql;
create table country_sites_2 as
select a.*
from country_sites a where COUNTRY in ("AE","FR","ES","DE","IT","AU","SG","HK","BR","JP","NZ","IE","GB","CN","MX","BE","NL","LU"
,"TH","MY","TW","KR")
and strip(site_type) in ("Retail");
quit;

data country_sites_2;
set country_sites_2;
if COUNTRY in ("BE","NL","LU") then country="Benelux";
run;

data country_sites_1;
set country_sites_2;
format flag $3.;
if external_site_id="" then flag="ST";
if external_site_id="" then smsbill=SMS_BILL_IND;
if external_site_id^="" then flag="FF";
if external_site_id^="" then smsbill="Y";
run;

data country_sites_1;
set country_sites_1;
where smsbill="Y";
run;



data country_sites_3;
set country_sites_1;
format ce_id $10.;
if external_site_id="" then ce_id=site_id;
if external_site_id^="" then ce_id=external_site_id;
run;

/*Excluding dummy sites*/
proc sql threads feedback;
create table country_sites_4 as
select * from country_sites_3 where ce_id not in (select external_site_id from raw.excluded_sites_122817);
quit;

proc sql threads feedback;
create table country_sites_5 as
select a.*,b.OrgID from country_sites_4 a left join raw.orgexclusionintrep_120418 b on 
a.organization_id=b.OrgID and a.country=b.country
;
quit;

data country_sites_5;
set country_sites_5;
where orgid=.;
run;

data country_sites_5;
set country_sites_5;
where chain_id ^=99999;
run;

/*exclude mango for Spain due to high chain share*/
data country_sites_5;
set country_sites_5;
if country="ES" and chain_id=12186 then chain_id=333333;
run;

data country_sites_5;
set country_sites_5;
drop orgID;
where chain_id^=333333;
run;

/*APR combined franchises for China*/
data country_sites_china;
set country_sites_5;
where country='CN';
run;

proc sql threads feedback;
create table country_sites_5_1 as
select a.*,b.new_org_id,b.new_org_name
from country_sites_china a left join combined_brands b
on a.organization_id=b.org_id;
quit;

data country_sites_5_1;
set country_sites_5_1;
format org_id_final best12.;
format org_name_final $55.;
org_id_final=organization_id;
org_name_final=organization_name;
if new_org_id^=. then org_id_final =new_org_id;
if new_org_name^="" then org_name_final =new_org_name;
drop organization_id organization_name new_org_id new_org_name;
rename org_id_final=organization_id;
rename org_name_final=organization_name;
run;

proc sql  threads feedback;
create table country_sites_china as
select site_id,chain_id,site_type,chain_name,city,state_prov,zip,country,sms_bill_ind,organization_id,organization_name,external_site_id,
flag,smsbill,ce_id
from country_sites_5_1;
quit;
data country_sites_5_2;
set country_sites_china country_sites_5;
run;

proc sort data=country_sites_5_2 nodupkey out=country_sites_6_national;
by site_id chain_id country;
quit;


proc sql threads feedback;
select flag,count(distinct site_id) as site_cnt
from country_sites_6_national
group by flag;
quit;

data country_sites_6_FR_Region_2;
set country_sites_6_national;
where country = "FR";
zip_2=substr(compress(strip(zip)," *-?,.:",""),1,2);
if substr(zip_2,1,1) in ('0','O') then zip_2=compress(zip_2,"0O","");
if substr(zip_2,1,1) in ('x') then zip_2="";
if zip_2 in ('NA') then zip_2="";
run;

PROC SQL THREADS FEEDBACK;
create table country_sites_6_FR_Region_3 as
select a.*,b.region
from country_sites_6_FR_Region_2 a inner join france_regions_postcodes b
on a.zip_2=b.postal_codes_start_by;
quit;

data country_sites_6_FR_Region;
set country_sites_6_FR_Region_3;
where region in ("ILE DE France","SOUTH WEST","SOUTH EAST");
country=region;
drop zip_2 region;
run;

proc sql;
	create table country_sites_6_city as
	select a.*
	from country_sites_6_national a where COUNTRY in ("FR","BR","GB","AU","CN","HK","JP","SG","DE","ES","IT","MY","AE")
	and city in ("Barcelona","Berlin","Glasgow","Hong Kong","London","Madrid","Manchester",
	"Paris","Sao Paulo","Tokyo","Roma","Singapore","Dubai","Kuala Lumpur")
	OR (country = "BR" and state_prov="SP")
	and strip(site_type) in ("Retail");
	quit;

data country_sites_6_city_1;
set country_sites_6_city;
if city in ("Singapore") then country="Singapore City";
if city in ("Sao Paulo") then country="Sao Paulo City";
if city in ("Barcelona","Berlin","Glasgow","Hong Kong","London","Madrid","Manchester","Paris","Tokyo","Roma","Dubai","Kuala Lumpur") then country=city;
	run;

data country_sites_6_city_2;
set country_sites_6_city;
if (country = "BR" and state_prov="SP")then country=state_prov;
run;

data country_sites_6;
set country_sites_6_national country_sites_6_FR_Region country_sites_6_city_1 country_sites_6_city_2;
run;
proc sql threads feedback;
create table allinfo_bysite as
select a.ce_id as site_id,a.log_ts as date_enter,a.total_enters,b.chain_id,b.organization_id as org_id,b.organization_name as org_name,b.flag
,b.country,b.site_type
from raw.ff_st_intl_&traffic_date. a inner join country_sites_6 b
on strip(a.ce_id)=strip(b.ce_id);
quit;


proc sql threads feebdack;
create table site_min_traffic as
select ce_id, min(log_ts) format ddmmyy10. as first_date from raw.ff_st_intl_&traffic_date.
group by ce_id;
quit;

proc sql threads feedback;
create table allinfo_bysite2 as
select a.site_id,a.org_id,a.org_name,a.country,a.site_type,a.flag,a.date_enter,a.date_enter-364 format ddmmyy10. as lag_date,
a.total_enters,b.total_enters as lag_enters,
c.first_date format ddmmyy10.
from allinfo_bysite a inner join allinfo_bysite b on a.site_id=b.site_id and a.date_enter=b.date_enter+364  and a.country=b.country
left join site_min_traffic c on strip(a.site_id)=strip(c.ce_id);
quit;
proc sql threads feedback;
create table allinfo_bysite3 as
select a.*,b.*
from allinfo_bysite2 a left join raw.nrf_calendar_020120 b
on a.date_enter=b.date
where date_enter>=&start_date. and date_enter<=&end_date.;
quit;

data allinfo_bysite3;
set allinfo_bysite3;
where total_enters^=. and lag_enters^=.;
run;

data allinfo_bysite4;
set allinfo_bysite3;
comp_ind1=(date_enter-first_date) >=371;
run;


data allinfo_bysite5;
set allinfo_bysite4;
where period_start-first_date >=371;
run;

proc sort data=allinfo_bysite5 nodupkey;
by site_id country date_enter;
run;

proc sql threads feedback;
create table reportability_metrics as
select 'National' as geography_level,country as geography_name,nrf_year,nrf_month,site_type as index_type
,count(distinct site_id) as distinct_sites,count(distinct org_id) as distinct_orgs,
sum(case when comp_ind1=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 
group by 'National',country,nrf_year,nrf_month,site_type;
quit;

proc sql threads feedback;
create table org_contrib1 as
select 'National' as geography_level,country as geography_name,nrf_year,nrf_month,site_type as index_type
,org_id,org_name,sum(total_enters) as org_enters,
count(distinct site_id) as org_sites
from allinfo_bysite5 group by 'National',country,nrf_year,nrf_month,org_id,org_name,site_type;
quit;

proc sql threads feedback;
create table major_orgs as
select geography_level,geography_name,nrf_year,nrf_month,max(org_enters) as major_org from org_contrib1
group by geography_level,geography_name,nrf_year,nrf_month;
quit;

proc sql threads feedback;
create table major_orgs1 as 
select a.*,b.org_id,b.org_name
from major_orgs a left join org_contrib1 b 
on a.geography_level = b.geography_level and a.geography_name = b.geography_name 
and a.nrf_year = b.nrf_year and a.nrf_month = b.nrf_month and a.major_org = b.org_enters;
quit;


proc sql threads feedback;
create table org_contrib2 as
select geography_level,geography_name,nrf_year,nrf_month,index_type,max(org_enters)/sum(org_enters)
as org_share,max(org_sites)/sum(org_sites) as org_site_share,sum(org_sites) as total_sites,sum(org_enters) as total_enters
from org_contrib1 group by geography_level,geography_name,nrf_year,nrf_month,index_type;
quit;

proc sql threads feedback;
create table org_summary as
select a.*,b.total_enters,b.total_sites,a.org_enters/b.total_enters as pct_chain_share,a.org_sites/b.total_sites as pct_chain_site_share
from org_contrib1 a left join org_contrib2 b
on a.geography_level = b.geography_level and a.geography_name = b.geography_name and a.nrf_year = b.nrf_year and a.nrf_month = b.nrf_month
and a.index_type = b.index_type;
quit;

proc sql threads feedback;
create table mall_contrib1 as
select 'National' as geography_level,country as geography_name,nrf_year,nrf_month,site_id,site_type as index_type,sum(total_enters) as mall_enters
from allinfo_bysite5 group by 'National',country,nrf_year,nrf_month,site_id,site_type;
quit;

proc sql threads feedback;
create table mall_contrib2 as
select geography_level,geography_name,nrf_year,nrf_month,index_type,max(mall_enters)/sum(mall_enters)
as mall_share
from mall_contrib1 group by geography_level,geography_name,nrf_year,nrf_month,index_type;
quit;

proc sql threads feedback;
create table final_rep as
select a.*,b.mall_share,c.org_share,c.org_site_share
from reportability_metrics a inner join mall_contrib2 b 
on a.nrf_month=b.nrf_month and a.nrf_year=b.nrf_year and a.geography_name=b.geography_name
and a.index_type=b.index_type
inner join org_contrib2 c on a.nrf_month=c.nrf_month and a.nrf_year=c.nrf_year and a.geography_name=c.geography_name
and a.index_type=c.index_type;
quit;


proc sql threads feedback;
create table stability_1 as
select 'National' as geography_level,country as geography_name,nrf_year,nrf_month,date_enter,site_type as index_type
,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5
group by 'National',country,nrf_year,nrf_month,date_enter,site_type;
quit;

data stability_1;
set stability_1 ;
stable_yoy=(abs(comp_yoy)<=0.2);
run;

proc sql threads feedback;
create table stability_2 as
select geography_level,geography_name,nrf_year,nrf_month,index_type,mean(stable_yoy) as pct_stable
from stability_1
group by geography_level,geography_name,nrf_year,nrf_month,index_type;
quit;


proc sql threads feedback;
create table final_rep_2 as
select a.*,b.pct_stable from
final_rep a left join stability_2 b
on a.geography_level=b.geography_level and a.geography_name=b.geography_name and a.nrf_month=b.nrf_month and a.nrf_year=b.nrf_year
and a.index_type=b.index_type;
quit;

data final_rep_2;
set final_rep_2;
if geography_name in ("ILE DE France","SOUTH WEST","SP","SOUTH EAST") then geography_level="Region";
if geography_name in ("Barcelona","Berlin","Glasgow","Hong Kong","London","Madrid","Manchester","Paris",
"Sao Paulo City","Tokyo","Roma","Singapore City","Dubai","Kuala Lumpur")
then geography_level="City";
run;

data final_rep_3;
set final_rep_2;
multiple_chains=(distinct_orgs>=3 & distinct_orgs^=.);
if index_type='Mall' and geography_level="National" then sufficient_sites=(avg_daily_sites>=20 & avg_daily_sites^=.);
if index_type='Mall' and (geography_level="Region" or geography_level="City") then sufficient_sites=(avg_daily_sites>=15 & avg_daily_sites^=.);
if index_type='Retail' and geography_level="National" then sufficient_sites=(avg_daily_sites>=50 & avg_daily_sites^=.);
if index_type='Retail' and (geography_level="Region" or geography_level="City") then sufficient_sites=(avg_daily_sites>=30 & avg_daily_sites^=.);
org_share_flag=(org_share<=0.5 & org_share^=.);
org_site_share_flag=(org_site_share<=0.5 & org_site_share^=.);
mall_share_flag=(mall_share<=0.5 & mall_share^=.);
stability=(pct_stable>=0.8 & pct_stable^=.);
reportable=(multiple_chains=1 & sufficient_sites=1 & org_share_flag=1 & org_site_share_flag=1 & mall_share_flag=1 & stability=1);
if index_type='Mall' then reportability_pct=(multiple_chains+sufficient_sites+org_share_flag+org_site_share_flag+stability+mall_share_flag)/6;
if index_type='Retail' then reportability_pct=(multiple_chains+sufficient_sites+org_share_flag+org_site_share_flag+stability)/5;
run;

proc sql threads feedback;
create table final_rep_4 as
select a.*,b.nrf_month_num from final_rep_3 a left join nrf_month_num b on a.nrf_month=b.nrf_month and a.nrf_year^=.;
quit;
proc sort data=final_rep_4;
by geography_level geography_name nrf_year nrf_month_num;
where nrf_year^=.;
run;



%let n = 7;

data mx_rep_sheet_final;
  set final_rep_4;
  by geography_level geography_name nrf_year nrf_month_num;
  retain rep_sum 0;
  if first.geography_name then do;
    count=0;
    rep_sum=0;
  end;
  count+1;
  last&n=lag&n(reportable);
  if count gt &n then rep_sum=sum(rep_sum,reportable,-last&n);
  else rep_sum=sum(rep_sum,reportable);
  if count ge &n then Past6MonthsRep=(rep_sum-reportable)/(&n-1);
  else Past6MonthsRep=.;
run;

data mx_rep_sheet_final;
set mx_rep_sheet_final;
format final_reportability best12.;
final_reportability=reportable;
if Past6MonthsRep=. then Past6MonthsRep=reportable;
if reportable=0 then final_reportability=(Past6MonthsRep>=0.8 & reportability_pct>=0.8);
drop rep_sum count last7;
where geography_name^= '';
run;

proc sql threads feedback;
create table combined_final_rep_sheet as
select geography_level,geography_Name,nrf_Year,NRF_Month,'Total Retail' as Segment,Distinct_Sites,Distinct_Orgs,
Avg_Daily_Sites,Org_Share,Org_Site_Share,pct_stable,
Multiple_chains,Sufficient_Sites,Org_Share_Flag,Org_Site_Share_Flag,Stability,Reportable,nrf_month_num,
reportability_pct, Past6MonthsRep, final_reportability
from mx_rep_sheet_final;
quit; 

data WORK.COUNTRY_codes                           ;
    %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
    infile 'Q:\Users\Pokkuluri117659\02.Data collection\01.Raw data\country_codes.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
       informat Code $2. ;
       informat Name $40. ;
       format Code $2. ;
       format Name $40. ;
    input
                Code $
                Name $
    ;
    if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
    run;

proc sql threads feedback;
create table rep_sheet_final as 
select a.geography_level,a.geography_Name,b.name,a.nrf_Year,a.NRF_Month,a.Segment,a.Distinct_Sites,a.Distinct_Orgs,
a.Avg_Daily_Sites,a.Org_Share,a.Org_Site_Share,a.pct_stable,a.Multiple_chains,a.Sufficient_Sites,
a.Org_Share_Flag,a.Org_Site_Share_Flag,a.Stability,a.Reportable,a.nrf_month_num,
a.reportability_pct, a.Past6MonthsRep, a.final_reportability 
from combined_final_rep_sheet a left join country_codes b 
on a.geography_name=b.code;
quit;
 
data rep_sheet_final;
set rep_sheet_final;
if name='' then name=geography_name;
drop geography_name;
rename name=geography_name;
run;

data rep_sheet_final;
set rep_sheet_final;
if geography_name in ("ILE DE France","SOUTH WEST","Sao Paulo","SOUTH EAST") then geography_level="Region";
if geography_name in ("Barcelona","Berlin","Glasgow","Hong Kong","London","Madrid","Manchester","Paris",
"Sao Paulo City","Tokyo","Roma","Singapore City","Dubai","Kuala Lumpur")
then geography_level="City";
run;

proc sql threads feedback;
create table org_summary_final as
select geography_level,geography_Name,nrf_Year,NRF_Month,'Total Retail' as Segment,Org_Id,Org_Name,Org_Enters,Org_Sites,
Total_Enters,Total_Sites,Pct_Chain_Share,Pct_Chain_Site_Share
from org_summary;
quit; 

proc sql threads feedback;
create table org_summary_final as 
select a.geography_level,a.geography_Name,b.name,a.nrf_Year,a.NRF_Month,a.Segment,a.Org_Id,a.Org_Name,a.Org_Enters,a.Org_Sites,
a.Total_Enters,a.Total_Sites,a.Pct_Chain_Share,a.Pct_Chain_Site_Share
from org_summary_final a left join country_codes b 
on a.geography_name=b.code;
quit;
 
data org_summary_final;
set org_summary_final;
if name='' then name=geography_name;
drop geography_name;
rename name=geography_name;
run;

data org_summary_final;
set org_summary_final;
if geography_name in ("ILE DE France","SOUTH WEST","Sao Paulo","SOUTH EAST") then geography_level="Region";
if geography_name in ("Barcelona","Berlin","Glasgow","Hong Kong","London","Madrid","Manchester","Paris",
"Sao Paulo City","Tokyo","Roma","Singapore City","Dubai","Kuala Lumpur")
then geography_level="City";
run;
data org_summary_final;
set org_summary_final;
if geography_name="ILE DE FRANCE" then geography_name="Ile de France";
if geography_name="SOUTH WEST" then geography_name="South West, France";
if geography_name="SOUTH EAST" then geography_name="South East, France"; 
run;
data rep_sheet_final;
set rep_sheet_final;
if geography_name="ILE DE France" then geography_name="Ile de France";
if geography_name="SOUTH WEST" then geography_name="South West, France";
if geography_name="SOUTH EAST" then geography_name="South East, France"; 
run;
proc export data=rep_sheet_final outfile="Q:\Users\Pokkuluri117659\batch_rep\&run_date._Reportability.csv" dbms=csv replace;
run;
proc export data=org_summary_final outfile="Q:\Users\Pokkuluri117659\batch_rep\&run_date._org_summary.csv" dbms=csv replace;
run;

