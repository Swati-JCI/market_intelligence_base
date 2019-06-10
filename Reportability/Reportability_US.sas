 /**********************************************************************
 *   PRODUCT:   SAS
 *   VERSION:   9.2
 *   CREATOR:   External File Interface
 *   DATE:      08AUG18
 *   DESC:      Generated SAS Datastep Code
 *   TEMPLATE SOURCE:  (None Specified.)
 ***********************************************************************/
    data WORK.NRF_MONTH_num                           ;
    %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
    infile 'Q:\Users\Srihita114499\02.DataCollection\02.01.RawData\month_order.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
       informat Month $11. ;
       informat Month_Num best32. ;
       format Month $11. ;
       format Month_Num best12. ;
    input
                Month $
                Month_Num
    ;
    if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
    run;
 /**********************************************************************
 *   PRODUCT:   SAS
 *   VERSION:   9.2
 *   CREATOR:   External File Interface
 *   DATE:      11JAN18
 *   DESC:      Generated SAS Datastep Code
 *   TEMPLATE SOURCE:  (None Specified.)
 ***********************************************************************/
    data WORK.market_names                            ;
    %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
    infile 'Q:\Users\Srihita114499\02.DataCollection\02.01.RawData\market_names.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
       informat Market_ID $9. ;
       informat Market_Name $15. ;
       format Market_ID $9. ;
       format Market_Name $15. ;
    input
                Market_ID $
                Market_Name $
    ;
    if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
    run;
 /**********************************************************************
 *   PRODUCT:   SAS
 *   VERSION:   9.2
 *   CREATOR:   External File Interface
 *   DATE:      11JAN18
 *   DESC:      Generated SAS Datastep Code
 *   TEMPLATE SOURCE:  (None Specified.)
 ***********************************************************************/
    
    data WORK.SEGMENT_name_mapping                    ;
    %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
    infile 'Q:\Users\Srihita114499\02.DataCollection\02.01.RawData\Segment_Name_Mapping.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
       informat SegCatType $8. ;
       informat SegmentID $6. ;
       informat SegmentName $30. ;
       format SegCatType $8. ;
       format SegmentID $6. ;
       format SegmentName $30. ;
    input
                SegCatType $
                SegmentID $
                SegmentName $
    ;
    if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
    run;
/*-------------------------------------------------------------------------------------------------------------------*/
/*------------------------------------------ INPUT USER START -------------------------------------------------------*/
/*-------------------------------------------------------------------------------------------------------------------*/
%let run_date=021119;
%let latest_traffic_date = 020319;
%let remedy_date = 020519;
%let start_date = mdy(02,04,2018);/*take the start of thet pas 7th month from the current month*/
%let end_date = mdy(02,02,2019);

/*proc sql threads feedback;*/
/*select count(distinct site_id) from raw.remedy_data_052716;*/
/*quit;*/
/*
The following dates should be based on the latest date that is present in the traffic file
*/
%LET current_date = 11;
%let current_month= 02;
%let current_year=2019 ;

/*Name of the Traffic and Remedy Data files needs to be changed*/
%let latest_remedy_data = Remedy_data_&remedy_date. ;
%let latest_traffic_data= Traffic_&latest_traffic_date.;

%let path =Q:\Project\03.DataPreparation\03.03.Others;

/*-------------------------------------------------------------------------------------------------------------------*/
/*------------------------------------------- INPUT USER ENDS -------------------------------------------------------*/
/*-------------------------------------------------------------------------------------------------------------------*/

options obs = max ls = max ps = max mergenoby = error nocenter noxwait noxsync macrogen mprint;
options mstored sasmstore = catlog symbolgen fullstimer;

libname raw "Q:\Project\02.DataCollection\02.02.ConvertedData";
libname base "Q:\Project\03.DataPreparation\03.02.Data\Comp_Store\Base Table"; 
libname output "Q:\Project\03.DataPreparation\03.02.Data\Comp_Store\Daily\Output Files";
libname repzip 'Q:\Users\Michael78740\03.DataPreparation\03.02.Data\Zip_models\Stability';
libname forecast "Q:\Users\Mangipudi82992\03.DataPreparation\03.01.Program\Forecasting\Datasets";
libname meena"Q:\Users\Mangipudi82992\03.DataPreparation\03.01.Program\US\Datasets\US_cat_Site_exc_datasets";
/*Use overridden chain_categories file i.e. overrriden for categories as well as chain ids using chain groupings*/
Proc Sql Threads Feedback;
	create table chain_cat_segment as
		select 	a.*,
				b.segment_id 
		from raw.chain_categories_020519 a
			left join raw.category_segment_110317 b
				on 	a.category_id=b.category_id 
					& a.active_ind^="N";
quit;

/*Step 4 & 5*/
                          * Sub-setting chain_cat_segment table for only 'segment_id' <=7;


data chain_segment ;
	set chain_cat_segment (keep= chain_id segment_id);
	where segment_id in (1,2,3,4,5,6,7);
run;

                                        *Assigning flag for special chains;
data special_chains ;
	set raw.special_chains ;
	special_chain_flag=1;
run;


                        *Assigning flag for 'chain_id' in compliance_exclusion table where 'exclusion_date=1;
data compliance_exclusion ;
	set raw.compliance_exclusion_032217 ;
	if exclusion_start_date ^= .
		then compliance_exclusion_flag=1;
run;


                               *Assigning flag for 'chain_id' present in chain_segment table;
data chain_segment ;
	set chain_segment ;
	chain_segment_flag=1;
run;

/*  data WORK.PETSMART_states                         ;*/
/*  %let _EFIERR_ = 0; /* set the ERROR detection macro variable */*/
/*  infile 'Q:\Users\Sayoni93677\02.DataCollection\02.01.RawData\061616_Petsmart_Regions.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;*/
/*     informat STATE_PROV $2. ;*/
/*     informat VAR2 $17. ;*/
/*     informat Petsmart_Region $13. ;*/
/*     format STATE_PROV $2. ;*/
/*     format VAR2 $17. ;*/
/*     format Petsmart_Region $13. ;*/
/*  input*/
/*              STATE_PROV $*/
/*              VAR2 $*/
/*              Petsmart_Region $*/
/*  ;*/
/*  if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */*/
/*  run;*/

                          *Mapping 'special_chain_flag' in remedy_data table from special_chains table;
Proc Sql Threads Feedback;
	create table remedy_data_v1
		as select a.*,
				  b.special_chain_flag
			from raw.remedy_data_020519 a 
			left join special_chains b
				on a.chain_id=b.chain_id;
quit;
                  *Mapping 'compliance_exclusion_flag' in remedy_data_v1 table from compliance_exclusion table;
Proc Sql Threads Feedback;
	create table remedy_data_v2 
		as select a.*,
				  b.compliance_exclusion_flag
			from remedy_data_v1 a 
			left join compliance_exclusion b
				on a.chain_id=b.chain_id;
quit;


                        *Mapping 'chain_segment_flag' in remedy_data_v2 table from chain_segment table;
Proc Sql Threads Feedback;
	create table remedy_data_v3 
		as select a.*,
				  b.chain_segment_flag
			from remedy_data_v2 a 
			left join chain_segment b
				on a.chain_id=b.chain_id;
quit;


                        *Removing "-" from 'zip' variable and changing its format from character to numeric 
                                    and sub-setting remedy_data_v3 to keep only useable sites;

data useable_sites ;
	set remedy_data_v3 ;
	zip_num= input(substr(zip,1,5),comma5.);
		where special_chain_flag ne 1 &
			  compliance_exclusion_flag ne 1 &
			  site_type="Retail" &
			  country="US" &
			  state_prov not in ("MP","GU","ON","PR","QC","VI") 
/*&*/
/*			  sms_bill_ind="Y"*/
;
	drop zip;
	rename zip_num=zip;
run;


					*Sub-setting useable_sites table by removing all non-available values of 'zip';
data useable_sites_v0 ;
	set useable_sites ;
	where zip^=.;
run;


/*Step 12 & 13*/
                 *Mapping 'segment_id' variable from chain_segment table to useable_sites_v0 table 
							and saving the final table at permanent location;
Proc Sql Threads Feedback;
		create table useable_sites_&run_date. as
			select 	a.*,
					b.segment_id 
			from useable_sites_v0 a
				inner join chain_segment b
					on a.chain_id=b.chain_id; 
Quit;


/* Merging sites with categories.Same site can be assigned to multiple categories .So we will get duplicates if we do nodup
on sites alone whereas if you do nodup on site and category ,we dont get any duplicates */

Proc Sql threads feedback;
		 create table community_useable_sites_chain as select a.*, b.category_id, c.category_name
		 from useable_sites_&run_date. a inner join raw.chain_category_overriden b on a.chain_id = b.chain_id
		 left join raw.category_segment_110317 c on b.CATEGORY_ID=c.CATEGORY_ID and c.category_id not in (36,37,38,39)
		  where c.segment_id in (1,2,3,4,5,6,7) and b.active_ind='Y'
		 order by site_id, category_id;
QUIT;

data community_useable_sites_chain;
set community_useable_sites_chain;
/*category_id=0; */
/*category_name="Total Retail";*/
run;
proc sql threads feedback;
create table qc as
select category_id,count(distinct site_id) from community_useable_sites_chain
group by category_id;
quit;
/* for combined categories*/
data WC_app_footware;
set community_useable_sites_chain;
if category_name="Shoes" or category_name="Children's Apparel" or category_name="Women's Apparel";
category_name="WC_App_foot_grp";
category_id="202122";
run;

data W_app_footware;
set community_useable_sites_chain;
if category_name="Shoes" or category_name="Women's Apparel";
category_name="W_App_foot_grp";
category_id="2022";
run;

data W_Jew_Acc;
set community_useable_sites_chain;
if category_name="Jewelry" or category_name="Accessories";
category_name="W_Jew_Acc_grp";
category_id="2425";
run;
data community_useable_sites_chain;
set community_useable_sites_chain wc_app_footware w_app_footware;
run;



/*Step 14-16*/

					*Sub-setting zip_mkt_region and assigning 'market_id" values for certain conditions;
data zip_mkt_region ;
	set raw.zip_cbsa_mkt  (keep= zip market_id region) ;
	if market_id=. & region="Midwest" then market_id=91;
	if market_id=. & region="Northeast" then market_id=92;
	if market_id=. & region="South" then market_id=93;
	if market_id=. & region="West" then market_id=94;
run;

Proc Sql Threads Feedback;
	Create Table community_useable_sites_chain as
	Select a.*,b.market_id, b.region From
	community_useable_sites_chain a left join zip_mkt_region b
	on a.zip=b.zip;
Quit;
data community_useable_sites_chain;
set community_useable_sites_chain;
if zip=1806 then region="Northeast"
run;

proc sort data=community_useable_sites_chain out=test1 nodupkey;
by site_id;
run;

/*--------------------------------------------------------------------------------------------------------------------*/
										*Step 17 & 18--DOES NOT EXIST YET;
/*--------------------------------------------------------------------------------------------------------------------*/


/*Step 19 & 20 */
				*Finding out the first date of all the sites present in the useables sites from overall traffic data 
										whose 'total_enters' is greater than 0;;
Proc Sql Threads Feedback;
	create table sites_first_day as
		select 	site_id, min_traffic_date as first_date 
		from raw.sites_first_day_020519
		/*from raw.weekly_traffic_data_10_11_12_13*/
			where 	site_id in (select site_id 
						from community_useable_sites_chain) 
			order by site_id;
quit;

				*Removing Duplicates from the latest 2 year traffic data on site_id and date_enter;
/**/
/*proc sort Threads data=raw.&latest_traffic_data. */
/*	out=traffic_data_deduped nodupkey;*/
/*		by ce_id log_ts;*/
/*run;*/


				*Sub-setting traffic data for only those 'site_id' which are present in Useable sites;
		*Changing the value of 'date_enter' as per SAS format (which is earlier in R format) and creating 'lag_date' variable;

Proc Sql Threads Feedback;
	create table enters as
		select 	ce_id as site_id,
				log_ts+3653 as date_enter format=mmddyy9.,
				total_enters,log_ts+3653-364 as lag_date format=mmddyy9.
		from raw.&latest_traffic_data.
			where ce_id in (select site_id 
								from community_useable_sites_chain);
quit;

proc sql threads feedback;
select max(date_enter)format mmddyy9. from enters;
quit;
*Sub-setting the enters table only for last 2 years of the traffic data;

Proc Sql Threads Feedback;
create table enters_past_364_days as 
	select *
	from enters 
	where date_enter >=mdy(02,01,2015) and date_enter<=&end_date.;
	
quit;

			*Renaming 'total_enters' as 'lag_enters' and careating new data from last 2 year enters data;

data WORK.SITE_SMS_HIST  (drop= create_date created_by last_update_date last_updated_by)                         ;
    %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
    infile 'Q:\Project\02.DataCollection\02.01.RawData\SMS_Bill_Hist\site_sms_history_020519.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
       informat SITE_ID best32. ;
       informat EFFECTIVE_DATE anydtdtm40. ;
       informat END_DATE anydtdtm40. ;
       informat SMS_BILL_IND $3. ;
       informat CREATE_DATE anydtdtm40. ;
       informat CREATED_BY $5. ;
       informat LAST_UPDATE_DATE anydtdtm40. ;
       informat LAST_UPDATED_BY $13. ;
       format SITE_ID best12. ;
       format EFFECTIVE_DATE datetime. ;
       format END_DATE datetime. ;
       format SMS_BILL_IND $3. ;
       format CREATE_DATE datetime. ;
       format CREATED_BY $5. ;
       format LAST_UPDATE_DATE datetime. ;
       format LAST_UPDATED_BY $13. ;
    input
                SITE_ID
                EFFECTIVE_DATE
                END_DATE
                SMS_BILL_IND $
                CREATE_DATE
                CREATED_BY $
                LAST_UPDATE_DATE
                LAST_UPDATED_BY $
    ;
	effective_date= datepart(effective_date);
	format effective_date mmddyy10.;
	end_date=datepart(end_date);
	format end_Date mmddyy10.;
	if end_Date=mdy(12,31,1999) then end_date=end_date+7200+3600;

    if _ERROR_ then call symputx('_EFIERR_',1);
	if sms_bill_ind='Y';/* set ERROR detection macro variable */
    run;

proc sql threads feedback;
create table enters_sms as
select a.* from enters_past_364_days a
inner join site_sms_hist b on a.site_id=b.site_id and (a.date_enter >= b.effective_date and a.date_enter <=b.end_date) and
b.sms_bill_ind='Y';
quit;



data enters_forlag (rename=total_enters=lag_enters);
	set enters_sms;
run;

/*Step 21*/

*  Mapping the following varibales from their respective tables in enters table :-

    VARIABLE                     TABLE                    JOINING KEY
1) chain_id  -----            Remedy_data  -------          site_id
2) Exclusion_date ----   compliance_exclusion  -----        chain_id
3) first_date  -------      sites_first_day   -------       site_id
4)lag_enters   ---------     enters_forlag   ----------  site_id & date_enter ;

Proc Sql Threads Feedback;
	create table enters1 as
	select 	a.*,
			b.chain_id,
			c.exclusion_date,
d.first_date,e.lag_enters
	from enters_sms a  
		inner join community_useable_sites_chain b
			on a.site_id=b.site_id
		left join raw.compliance_exclusion c
			on b.chain_id=c.chain_id
		left join sites_first_day d
			on a.site_id=d.site_id
		left join enters_forlag e
			on 	a.site_id=e.site_id 
				& a.lag_date=e.date_enter;
quit;

proc sql threads feedback;
select max(date_enter)format mmddyy9. from enters1;
quit;
						*Sorting Enters1 table on 'site_id' and 'date_enter';

proc sort Threads data=enters1;	by 	site_id date_enter;
run;

	

Proc Sql Threads Feedback;
	create table allinfo_bysite as
		select 	a.*,
				b.zip,b.city, b.market_id, b.region, b.category_name,
				b.segment_id,b.category_id
		from enters1 a, community_useable_sites_chain b
				where a.site_id=b.site_id;
quit;

proc sql threads feedback;
select max(date_enter) format mmddyy9. from allinfo_bysite;
quit;

				* Re-assgining values of 'region', 'market_id' and 'region_id' based upon certain cases ;

Proc Sql Threads Feedback;
	alter table allinfo_bysite
		add nation num, region_id num;
	update allinfo_bysite
		set nation=1,
		region=case
			when zip=1806 then "Northeast"
				else region
		end,
		market_id=case
			when zip=1806 then 3
				else market_id
		end,
		region_id=case
			when region="Midwest" then 1
			when region="Northeast" then 2
			when region="South" then 3
			when region="West" then 4
			else .
	end;
quit;

data allinfo_bysite;
set allinfo_bysite;
format nation best12.;
format region_id best12.;
nation=1;
if zip=1806 then region="Northeast";
select (region);
   when ('Midwest')     region_id=1;
   when ('Northeast')   region_id=2;
   when ('South')       region_id=3;
   when ('West')        region_id=4;
   otherwise            region_id=.;
end;
where date_enter>=mdy(02,01,2015);
run;

data allinfo_bysite;
set allinfo_bysite;
if zip=1806 then region="Northeast";
where date_enter>=mdy(02,01,2015);
run;

proc sql threads feedback;
create table allinfo_bysite2 as
select a.*,b.*
from allinfo_bysite a left join raw.nrf_calendar_updated_020219 b
on a.date_enter=b.date
;
quit;
/* removing specific chains and overriding chain_ids*/

data chainid_override_110217;
set raw.chainid_override_110217;
where chain_id^=. and chain_id^=99999;
run;

proc sort data=chainid_override_110217 nodupkey;
by chain_id new_chain_id;
run;


proc sql threads feedback;
create table allinfo_bysite3 as
select a.*,b.new_chain_id from allinfo_bysite2 a left join chainid_override_110217 b on
a.chain_id=b.chain_id ;
quit;


data allinfo_bysite3;
set allinfo_bysite3;
if new_chain_id^=. then chain_id=new_chain_id;
run;

data allinfo_bysite3;
set allinfo_bysite3;
where chain_id^=99999;
run;

data allinfo_bysite3;
set allinfo_bysite3;
where total_enters^=. and lag_enters^=.;
run;
/* checking the condition that site should be present for at least a week before compare date*/
data allinfo_bysite4;
set allinfo_bysite3;
comp_ind=(date_enter-first_date) >=371;
run;


data allinfo_bysite5;
set allinfo_bysite4;
where period_start-first_date >=371;
run;

/*creating reportability metrics*/
proc sql threads feedback;
create table reportability_metrics as
/*total retail*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,'Segment' as segcattype,'00' as segment_id,count(distinct site_id) as distinct_sites
,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1 
group by 'National','National',nrf_year,nrf_month,'00','Segment'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,'Segment' as segcattype,'00' as segment_id,count(distinct site_id) as distinct_sites
,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1 
group by 'Region',region,nrf_year,nrf_month,'00','Segment'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,'Segment' as segcattype,'00' as segment_id
,count(distinct site_id) as distinct_sites,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1
group by 'Market',market_id,nrf_year,nrf_month,'00','Segment'
union
/*segment*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,'Segment' as segcattype,put(segment_id,2.),count(distinct site_id) as distinct_sites
,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1 
group by 'National','National',nrf_year,nrf_month,segment_id,'Segment'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,'Segment' as segcattype,put(segment_id,2.),count(distinct site_id) as distinct_sites
,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1 
group by 'Region',region,nrf_year,nrf_month,segment_id,'Segment'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,'Segment' as segcattype,put(segment_id,2.),count(distinct site_id) as distinct_sites,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1
group by 'Market',market_id,nrf_year,nrf_month,segment_id,'Segment'
union
/*category*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,'Category' as segcattype,put(category_id,2.) as segment_id,count(distinct site_id) as distinct_sites
,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1 
group by 'National','National',nrf_year,nrf_month,category_id,'Category'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,'Category' as segcattype,put(category_id,2.) as segment_id,count(distinct site_id) as distinct_sites
,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1 
group by 'Region',region,nrf_year,nrf_month,category_id,'Category'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,'Category' as segcattype,put(category_id,2.) as segment_id
,count(distinct site_id) as distinct_sites,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1
group by 'Market',market_id,nrf_year,nrf_month,category_id,'Category'
;
quit;
proc sql threads feedback;
create table chain_contrib_1 as
/*total retail*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,chain_id,'Segment' as segcattype,'00' as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 group by 'National','National',nrf_year,nrf_month,chain_id,'00','Segment'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,chain_id,'Segment' as segcattype,'00' as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 group by 'Region',region,nrf_year,nrf_month,chain_id,'00','Segment'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,chain_id,'Segment' as segcattype,'00' as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 group by 'Market',market_id,nrf_year,nrf_month,chain_id,'00','Segment'
union
/*segment*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,chain_id,'Segment' as segcattype,put(segment_id,2.),sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 
/*where segment_id in (6,7)*/
group by 'National','National',nrf_year,nrf_month,chain_id,segment_id,'Segment'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,chain_id,'Segment' as segcattype,put(segment_id,2.),sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 
/*where segment_id in (6,7)*/
group by 'Region',region,nrf_year,nrf_month,chain_id,segment_id,'Segment'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,chain_id,'Segment' as segcattype,put(segment_id,2.),sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 
/*where segment_id in (6,7)*/
group by 'Market',market_id,nrf_year,nrf_month,chain_id,segment_id,'Segment'
union
/*category*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,chain_id,'Category' as segcattype,put(category_id,2.) as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 
/*where category_id in (4,5,20,22,23,24,25,45)*/
group by 'National','National',nrf_year,nrf_month,chain_id,category_id,'Category'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,chain_id,'Category' as segcattype,put(category_id,2.) as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 
/*where category_id in (4,5,20,22,23,24,25,45)*/
group by 'Region',region,nrf_year,nrf_month,chain_id,category_id,'Category'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,chain_id,'Category' as segcattype,put(category_id,2.) as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 
/*where category_id in (4,5,20,22,23,24,25,45)*/
group by 'Market',market_id,nrf_year,nrf_month,chain_id,category_id,'Category'
;
quit;
/**/
/*proc sql threads feedback;*/
/*create table temp as */
/*select geography_level,geography_name,nrf_year,nrf_month,category_id,sum(chain_enters) as month_enters,sum(chain_sites) as month_sites*/
/*from chain_contrib_cat*/
/*group by geography_level,geography_name,nrf_year,nrf_month,category_id;*/
/*quit;*/
/**/
/*proc sql threads feedback;*/
/*create table chain_contrib_cat1 as */
/*select a.*,b.month_enters,a.chain_enters/b.month_enters as chain_share,b.month_sites,a.chain_sites/b.month_sites as chain_site_share*/
/*from chain_contrib_cat a left join temp b*/
/*on a.geography_level = b.geography_level and a.geography_name = b.geography_name and a.nrf_year = b.nrf_year and a.nrf_month = b.nrf_month*/
/*and a.category_id = b.category_id ;*/
/*quit;*/
/**/
/*proc sort data = chain_contrib_cat1;*/
/*by nrf_year nrf_month category_id chain_id;*/
/*run;*/
/*PROC TRANSPOSE DATA=chain_contrib_cat1 OUT=temp1  NAME=geography_name;*/
/*BY nrf_year nrf_month category_id chain_id;*/
/*VAR chain_enters chain_share chain_sites chain_site_share;*/
/*ID geography_name;*/
/*RUN*/
/*;*/

* Output result to Excel;
/*union*/
/*select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,chain_id,category_id,sum(total_enters) as chain_enters,*/
/*count(distinct site_id) as chain_sites*/
/*from allinfo_bysite5 where category_id in (4,5,20,22,23,24,25,45)*/
/*group by 'Market',market_id,nrf_year,nrf_month,chain_id,category_id*/
/*;*/
/*quit;*/

proc sql threads feedback;
create table chain_contrib2 as
select geography_level,geography_name,nrf_year,nrf_month,segcattype,segment_id,max(chain_enters)/sum(chain_enters)
as chain_share,max(chain_sites)/sum(chain_sites) as chain_site_chare,sum(chain_sites) as total_sites,sum(chain_enters) as total_enters
from chain_contrib_1 group by geography_level,geography_name,nrf_year,nrf_month,segcattype,segment_id
;
quit;

proc sql threads feedback;
create table chain_summary as 
select a.*,b.total_enters,b.total_sites,a.chain_enters/b.total_enters as pct_chain_share,a.chain_sites/b.total_sites as pct_chain_site_share
from chain_contrib_1 a left join chain_contrib2 b
on a.geography_level = b.geography_level and a.geography_name = b.geography_name and a.nrf_year = b.nrf_year and a.nrf_month = b.nrf_month
and a.segment_id = b.segment_id and a.segcattype = b.segcattype;
quit;

proc sql threads feedback;
create table final_rep as
select a.*,b.chain_share ,b.chain_site_chare as chain_site_share
from reportability_metrics a inner join chain_contrib2 b
on a.nrf_month=b.nrf_month and a.nrf_year=b.nrf_year and a.geography_name=b.geography_name
and a.segment_id=b.segment_id and a.segcattype = b.segcattype and a.geography_level=b.geography_level
;
quit;
proc sql threads feedback;
create table stability_1 as
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,date_enter,'Segment' as segcattype,
'00' as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5
group by 'National','National',nrf_year,nrf_month,date_enter,'00','Segment'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,date_enter,'Segment' as segcattype,'00' as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5
group by 'Region',region,nrf_year,nrf_month,date_enter,'00','Segment'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,date_enter,'Segment' as segcattype,'00' as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5
group by 'Market',market_id,nrf_year,nrf_month,date_enter,'00','Segment'
union
/*segment*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,date_enter,'Segment' as segcattype,put(segment_id,2.),round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5
group by 'National','National',nrf_year,nrf_month,date_enter,segment_id,'Segment'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,date_enter,'Segment' as segcattype,put(segment_id,2.),round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5
group by 'Region',region,nrf_year,nrf_month,date_enter,segment_id,'Segment'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,date_enter,'Segment' as segcattype,put(segment_id,2.),round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5
group by 'Market',market_id,nrf_year,nrf_month,date_enter,segment_id,'Segment'
union
/*category*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,date_enter,'Category' as segcattype,put(category_id,2.) as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5
group by 'National','National',nrf_year,nrf_month,date_enter,segment_id,'Category' 
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,date_enter,'Category' as segcattype,put(category_id,2.) as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5
group by 'Region',region,nrf_year,nrf_month,date_enter,segment_id,'Category' 
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,date_enter,'Category' as segcattype,put(category_id,2.) as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5
group by 'Market',market_id,nrf_year,nrf_month,date_enter,segment_id,'Category' 
;
quit;

data stability_1;
set stability_1 ;
stable_yoy=(abs(comp_yoy)<=0.2);
run;
proc sql threads feedback;
create table stability_2 as
select geography_level,geography_name,nrf_year,nrf_month,segcattype,segment_id,mean(stable_yoy) as pct_stable
from stability_1
group by geography_level,geography_name,nrf_year,nrf_month,segment_id,segcattype;
quit;
proc sql threads feedback;
create table final_rep1 as
select a.*,b.pct_stable
from final_rep a inner join stability_2 b
on a.nrf_month=b.nrf_month and a.nrf_year=b.nrf_year and a.geography_name=b.geography_name and a.geography_level = b.geography_level
and a.segment_id=b.segment_id and a.segcattype = b.segcattype
;
quit;
/*Generation of reportable flag and reportability percentage*/
data final_rep1;
set final_rep1;
multiple_chains=(distinct_chains>=3 & distinct_chains^=.);
sufficient_sites=(avg_daily_sites>=50 & avg_daily_sites^=.);
chain_share_flag=(chain_share<=0.5 & chain_share^=.);
chain_site_share_flag=(chain_site_share<=0.5 & chain_site_share^=.);
stability=(pct_stable>=0.8 & pct_stable^=.);
reportable=(multiple_chains=1 & sufficient_sites=1 & chain_share_flag=1 & chain_site_share_flag=1 & stability=1);
reportability_pct=(multiple_chains+sufficient_sites+chain_share_flag+chain_site_share_flag+stability)/5;
run;
proc sql threads feedback;
create table final_rep2 as 
select a.*,b.SegmentName,c.market_name from final_rep1 a left join segment_name_mapping b
on strip(a.segment_id)=strip(b.SegmentID) and strip(a.segcattype)=strip(b.SegCattype)
left join market_names c on strip(a.geography_name) = strip(c.market_id);
quit;
data final_rep2;
set final_rep2;
drop geography_name;
rename market_name = geography_name;
where geography_name ^='.' and geography_name^='';
run;


/*reportability metrics for combined categories*/
proc sql threads feedback;
create table reportability_metrics_cc as
/*jewelry+Accessories*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,'Category' as segcattype,'2425' as segment_id,count(distinct site_id) as distinct_sites
,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1 and category_id in (24,25)
group by 'National','National',nrf_year,nrf_month,'2425','Category'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,'Category' as segcattype,'2425' as segment_id,count(distinct site_id) as distinct_sites
,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1 and category_id in (24,25)
group by 'Region',region,nrf_year,nrf_month,'2425','Category'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,'Category' as segcattype,'2425' as segment_id
,count(distinct site_id) as distinct_sites,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1 and category_id in (24,25)
group by 'Market',market_id,nrf_year,nrf_month,'2425','Category'
union
/*202122 grp*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,'Category' as segcattype,'202122' as segment_id,count(distinct site_id) as distinct_sites
,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1 and category_id in (20,21,22)
group by 'National','National',nrf_year,nrf_month,'202122','Category'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,'Category' as segcattype,'202122' as segment_id,count(distinct site_id) as distinct_sites
,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1 and category_id in (20,21,22)
group by 'Region',region,nrf_year,nrf_month,'202122','Category'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,'Category' as segcattype,'202122' as segment_id
,count(distinct site_id) as distinct_sites,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1 and category_id in (20,21,22)
group by 'Market',market_id,nrf_year,nrf_month,'202122','Category'
union
/*2022grp*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,'Category' as segcattype,'2022' as segment_id,count(distinct site_id) as distinct_sites
,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1 and category_id in (20,22)
group by 'National','National',nrf_year,nrf_month,'2022','Category'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,'Category' as segcattype,'2022' as segment_id,count(distinct site_id) as distinct_sites
,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1 and category_id in (20,22)
group by 'Region',region,nrf_year,nrf_month,'2022','Category'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,'Category' as segcattype,'2022' as segment_id
,count(distinct site_id) as distinct_sites,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1 and category_id in (20,22)
group by 'Market',market_id,nrf_year,nrf_month,'2022','Category'
;
quit; 
proc sql threads feedback;
create table chain_contrib_1_cc as
/*Jewelry+Accessories*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,chain_id,'Category' as segcattype,'2425' as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 where category_id in (24,25)
group by 'National','National',nrf_year,nrf_month,chain_id,'2425','Category'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,chain_id,'Category' as segcattype,'2425' as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 where category_id in (24,25)
group by 'Region',region,nrf_year,nrf_month,chain_id,'2425','Category'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,chain_id,'Category' as segcattype,'2425' as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 where category_id in (24,25)
group by 'Market',market_id,nrf_year,nrf_month,chain_id,'2425','Category'
union
/*202122 grp*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,chain_id,'Category' as segcattype,'202122' as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 where category_id in (20,21,22)
group by 'National','National',nrf_year,nrf_month,chain_id,'202122','Category'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,chain_id,'Category' as segcattype,'202122' as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 where category_id in (20,21,22)
group by 'Region',region,nrf_year,nrf_month,chain_id,'202122','Category'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,chain_id,'Category' as segcattype,'202122' as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 where category_id in (20,21,22)
group by 'Market',market_id,nrf_year,nrf_month,chain_id,'202122','Category'
union
/*2022 grp*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,chain_id,'Category' as segcattype,'2022' as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 where category_id in (20,22)
group by 'National','National',nrf_year,nrf_month,chain_id,'2022','Category'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,chain_id,'Category' as segcattype,'2022' as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 where category_id in (20,22)
group by 'Region',region,nrf_year,nrf_month,chain_id,'2022','Category'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,chain_id,'Category' as segcattype,'2022' as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 where category_id in (20,22)
group by 'Market',market_id,nrf_year,nrf_month,chain_id,'2022','Category'
;
quit;
proc sql threads feedback;
create table chain_contrib2_cc as
select geography_level,geography_name,nrf_year,nrf_month,segcattype,segment_id,max(chain_enters)/sum(chain_enters)
as chain_share,max(chain_sites)/sum(chain_sites) as chain_site_chare,sum(chain_sites) as total_sites,sum(chain_enters) as total_enters
from chain_contrib_1_cc group by geography_level,geography_name,nrf_year,nrf_month,segcattype,segment_id
;
quit;

proc sql threads feedback;
create table chain_summary_cc as 
select a.*,b.total_enters,b.total_sites,a.chain_enters/b.total_enters as pct_chain_share,a.chain_sites/b.total_sites as pct_chain_site_share
from chain_contrib_1_cc a left join chain_contrib2_cc b
on a.geography_level = b.geography_level and a.geography_name = b.geography_name and a.nrf_year = b.nrf_year and a.nrf_month = b.nrf_month
and a.segment_id = b.segment_id and a.segcattype = b.segcattype;
quit;

proc sql threads feedback;
create table final_rep_cc as
select a.*,b.chain_share ,b.chain_site_chare as chain_site_share
from reportability_metrics_cc a inner join chain_contrib2_cc b
on a.nrf_month=b.nrf_month and a.nrf_year=b.nrf_year and a.geography_name=b.geography_name
and a.segment_id=b.segment_id and a.segcattype = b.segcattype
;
quit;

proc sql threads feedback;
create table stability_1_cc as
/*2425*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,date_enter,'Category' as segcattype,'2425' as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5 where category_id in (24,25)
group by 'National','National',nrf_year,nrf_month,date_enter,'2425','Category'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,date_enter,'Category' as segcattype,'2425' as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5 where category_id in (24,25)
group by 'Region',region,nrf_year,nrf_month,date_enter,'2425','Category'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,date_enter,'Category' as segcattype,'2425' as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5 where category_id in (24,25)
group by 'Market',market_id,nrf_year,nrf_month,date_enter,'2425','Category'
union
/*202122*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,date_enter,'Category' as segcattype,'202122' as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5 where category_id in (20,21,22)
group by 'National','National',nrf_year,nrf_month,date_enter,'202122','Category'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,date_enter,'Category' as segcattype,'202122' as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5 where category_id in (20,21,22)
group by 'Region',region,nrf_year,nrf_month,date_enter,'202122','Category'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,date_enter,'Category' as segcattype,'202122' as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5 where category_id in (20,21,22)
group by 'Market',market_id,nrf_year,nrf_month,date_enter,'202122','Category'
union
/*2022*/
select 'National' as geography_level,'National' as geography_name,nrf_year,nrf_month,date_enter,'Category' as segcattype,'2022' as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5 where category_id in (20,22)
group by 'National','National',nrf_year,nrf_month,date_enter,'2022','Category'
union
select 'Region' as geography_level,region as geography_name,nrf_year,nrf_month,date_enter,'Category' as segcattype,'2022' as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5 where category_id in (20,22)
group by 'Region',region,nrf_year,nrf_month,date_enter,'2022','Category'
union
select 'Market' as geography_level,put(market_id,2.) as geography_name,nrf_year,nrf_month,date_enter,'Category' as segcattype,'2022' as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5 where category_id in (20,22)
group by 'Market',market_id,nrf_year,nrf_month,date_enter,'2022','Category'
;
quit;
data stability_1_cc;
set stability_1_cc ;
stable_yoy=(abs(comp_yoy)<=0.2);
run;
proc sql threads feedback;
create table stability_2_cc as
select geography_level,geography_name,nrf_year,nrf_month,segcattype,segment_id,mean(stable_yoy) as pct_stable
from stability_1_cc
group by geography_level,geography_name,nrf_year,nrf_month,segment_id,segcattype;
quit;
proc sql threads feedback;
create table final_rep1_cc as
select a.*,b.pct_stable
from final_rep_cc a inner join stability_2_cc b
on a.nrf_month=b.nrf_month and a.nrf_year=b.nrf_year and a.geography_name=b.geography_name and a.geography_level = b.geography_level
and a.segment_id=b.segment_id and a.segcattype = b.segcattype
;
quit;
data final_rep1_cc;
set final_rep1_cc;
multiple_chains=(distinct_chains>=3 & distinct_chains^=.);
sufficient_sites=(avg_daily_sites>=50 & avg_daily_sites^=.);
chain_share_flag=(chain_share<=0.5 & chain_share^=.);
chain_site_share_flag=(chain_site_share<=0.5 & chain_site_share^=.);
stability=(pct_stable>=0.8 & pct_stable^=.);
reportable=(multiple_chains=1 & sufficient_sites=1 & chain_share_flag=1 & chain_site_share_flag=1 & stability=1);
reportability_pct=(multiple_chains+sufficient_sites+chain_share_flag+chain_site_share_flag+stability)/5;
run;
proc sql threads feedback;
create table final_rep2_cc as 
select a.*,b.SegmentName,c.market_name from final_rep1_cc a left join segment_name_mapping b
on strip(a.segment_id)=strip(b.SegmentID) and strip(a.segcattype)=strip(b.SegCattype)
left join market_names c on strip(a.geography_name) = strip(c.market_id);
quit;
data final_rep2_cc;
set final_rep2_cc;
drop geography_name;
rename market_name = geography_name;
where geography_name ^=' .' and geography_name^='';
run;
data final_rep3;
set final_rep2_cc final_rep2;
run;
data chainsum;
set chain_summary chain_summary_cc;
where nrf_year=2018 and nrf_month="January";
run;
proc sql threads feedback;
create table final_rep_4 as
select a.*,b.month_num as nrf_month_num from final_rep3 a left join nrf_month_num b on a.nrf_month=b.month;
quit;
proc sql threads feedback;
create table region_names as
select geography_level,geography_name,count(*) as count from final_rep_4 
group by geography_level,geography_name;
quit;
data region_names;
set region_names;
drop count;
run;

proc sql threads feedback;
create table nrf_years as
select distinct nrf_year from final_rep_4 where nrf_year ^=.;
quit;
proc sql threads feedback;
create table nrf_months as
select nrf_month,nrf_month_num,count(*) as count from final_rep_4 where nrf_month ^=''
group by nrf_month,nrf_month_num;
quit;
data nrf_months;
set nrf_months;
drop count;
run;
proc sql threads feedback;
create table segtype  as
select distinct segcattype from final_rep_4;
quit;
proc sql threads feedback;
create table segs as
select segcattype,segment_id,count(*) as count from final_rep_4 where segment_id ^='.' group by segcattype,segment_id;
quit;
data segs;
set segs;
drop count;
run;

proc sql threads feedback;
create table us_rep_sheet_skeleton as
select * From
	region_names , nrf_years,nrf_months,segs;
quit;
data temp1;
set us_rep_sheet_skeleton;
where (nrf_year=2018 and nrf_month_num<=12);
run;
data temp2;
set us_rep_sheet_skeleton;
where nrf_year^=2018;
run;
data us_rep_skeleton;
set temp2 temp1;
run;
proc sql threads feedback;
create table us_rep_sheet as
select a.geography_level,a.geography_name,a.nrf_year,a.nrf_month,a.nrf_month_num,a.segcattype,a.segment_id,b.* From
us_rep_skeleton a left join final_rep_4 b on 
strip(a.geography_level)=strip(b.geography_level) and
strip(a.geography_name)=strip(b.geography_name) and 
a.nrf_year=b.nrf_year and
a.nrf_month=b.nrf_month and
a.nrf_month_num=b.nrf_month_num and 
a.segcattype=b.segcattype and 
a.segment_id=b.segment_id and a.segment_id^=" ";
quit;
proc sql threads feedback;
create table temp as
select geography_level,geography_name,nrf_year,nrf_month,nrf_month_num,segcattype,segment_id,count(*) from us_rep_sheet
group by geography_level,geography_name,nrf_year,nrf_month,nrf_month_num,segcattype,segment_id having count(*)>1;
quit;
proc sort data=us_rep_sheet;
by geography_level geography_name segcattype segment_id  nrf_year nrf_month_num ;
run;
/*data us_rep_sheet;*/
/*set us_rep_sheet;*/
/*where segcattype^="";*/
/*run;*/
%let n = 7;
data us_rep_sheet;
set us_rep_sheet;
format concat $50.;
concat=catx('-',geography_name,segment_id,segcattype);
run;
proc sort data=us_rep_sheet;
by geography_level concat nrf_year nrf_month_num ;
run;
data us_rep_sheet_1;
  set us_rep_sheet;
  by geography_level concat nrf_year nrf_month_num;
  retain rep_sum 0;
  if first.concat then do;
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

data us_rep_sheet_1;
set us_rep_sheet_1;
format final_reportability best12.;
final_reportability=reportable;
if Past6MonthsRep=. then Past6MonthsRep=reportable;
if reportable=0 then final_reportability=(Past6MonthsRep>=0.8 & reportability_pct>=0.8);
drop rep_sum count last7;
where geography_name^= '';
run;

data us_rep_sheet_2;
  set us_rep_sheet_1;
  by geography_level concat nrf_year nrf_month_num;
  retain sites_sum 0;
  if first.concat then do;
    count=0;
    sites_sum=0;
  end;
  count+1;
  last&n=lag&n(avg_daily_sites);
  if count gt &n then sites_sum=sum(sites_sum,avg_daily_sites,-last&n);
  else sites_sum=sum(sites_sum,avg_daily_sites);
  if count ge &n then Past6MonthsSites=(sites_sum-avg_daily_sites)/(&n-1);
  else Past6MonthsSites=.;
run;
data us_rep_sheet_3;
  set us_rep_sheet_2(drop= sites_sum count last7);
  by geography_level concat nrf_year nrf_month_num;
  retain chains_sum 0;
  if first.concat then do;
    count=0;
    chains_sum=0;
  end;
  count+1;
  last&n=lag&n(distinct_chains);
  if count gt &n then chains_sum=sum(chains_sum,distinct_chains,-last&n);
  else chains_sum=sum(chains_sum,distinct_chains);
  if count ge &n then Past6MonthsChains=(chains_sum-distinct_chains)/(&n-1);
  else Past6MonthsChains=.;
run;
data us_rep_sheet_4;
  set us_rep_sheet_3(drop= chains_sum count last7);
  by geography_level concat nrf_year nrf_month_num;
  retain chainshare_sum 0;
  if first.concat then do;
    count=0;
    chainshare_sum=0;
  end;
  count+1;
  last&n=lag&n(chain_share);
  if count gt &n then chainshare_sum=sum(chainshare_sum,chain_share,-last&n);
  else chainshare_sum=sum(chainshare_sum,chain_share);
  if count ge &n then Past6MonthsChainshare=(chainshare_sum-chain_share)/(&n-1);
  else Past6MonthsChainshare=.;
run;
data us_rep_sheet_5;
  set us_rep_sheet_4(drop= chainshare_sum count last7);
  by geography_level concat nrf_year nrf_month_num;
  retain chainsiteshare_sum 0;
  if first.concat then do;
    count=0;
    chainsiteshare_sum=0;
  end;
  count+1;
  last&n=lag&n(chain_site_share);
  if count gt &n then chainsiteshare_sum=sum(chainsiteshare_sum,chain_site_share,-last&n);
  else chainsiteshare_sum=sum(chainsiteshare_sum,chain_site_share);
  if count ge &n then Past6MonthsChainsiteshare=(chainsiteshare_sum-chain_site_share)/(&n-1);
  else Past6MonthsChainsiteshare=.;
run;
data us_rep_sheet_6;
  set us_rep_sheet_5(drop= chainsiteshare_sum count last7);
  by geography_level concat nrf_year nrf_month_num;
  retain stable_sum 0;
  if first.concat then do;
    count=0;
    stable_sum=0;
  end;
  count+1;
  last&n=lag&n(pct_stable);
  if count gt &n then stable_sum=sum(stable_sum,pct_stable,-last&n);
  else stable_sum=sum(stable_sum,pct_stable);
  if count ge &n then Past6MonthsStability=(stable_sum-pct_stable)/(&n-1);
  else Past6MonthsStability=.;
run;
/*Reportable Since Flag calculation*/
%let n = 13;
data us_rep_sheet_2;
  set us_rep_sheet_1;
  by geography_level concat nrf_year nrf_month_num;
  retain rep_sum 0;
  if first.concat then do;
    count=0;
    rep_sum=0;
  end;
  count+1;
  last&n=lag&n(final_reportability);
  if count gt &n then rep_sum=sum(rep_sum,final_reportability,-last&n);
  else rep_sum=sum(rep_sum,final_reportability);
  if count ge &n then Past12MonthsRep=(rep_sum-final_reportability)/(&n-1);
  else Past12MonthsRep=.;
run;

data us_rep_sheet_2;
set us_rep_sheet_2;
drop rep_sum count last13;
run;


%let n = 25;
data us_rep_sheet_3;
  set us_rep_sheet_2;
  by geography_level concat nrf_year nrf_month_num;
  retain rep_sum 0;
  if first.concat then do;
    count=0;
    rep_sum=0;
  end;
  count+1;
  last&n=lag&n(final_reportability);
  if count gt &n then rep_sum=sum(rep_sum,final_reportability,-last&n);
  else rep_sum=sum(rep_sum,final_reportability);
  if count ge &n then Past24MonthsRep=(rep_sum-final_reportability)/(&n-1);
  else Past24MonthsRep=.;
run;

data us_rep_sheet_3;
set us_rep_sheet_3;
drop count rep_sum last25;
run;

proc sql threads feedback;
create table tot_month as
select geography_level,geography_name,segcattype,segment_id,segmentname,sum(final_reportability)/count(final_reportability) as pct_tot_months_rep
from us_rep_sheet_3 group by geography_level,geography_name,segcattype,segment_id,segmentname;
quit;

proc sql threads feedback;
create table final_rep as
select a.*,b.pct_tot_months_rep from us_rep_sheet_3 a left join tot_month b 
on a.geography_level =b.geography_level and a.geography_name=b.geography_name and a.segcattype=b.segcattype and a.segment_id=b.segment_id;
quit;

data final_rep_1;
set final_rep;
where nrf_year=2018 and nrf_month_num=1;
format reportable_since mmddyy8.;
reportable_since=mdy(1,1,2099);
if pct_tot_months_rep >=0.8 then reportable_since=mdy(2,1,2015);
if Past24MonthsRep >=0.8 and pct_tot_months_rep <0.8 then reportable_since=mdy(1,31,2016);
if Past12MonthsRep>=0.8 and Past24MonthsRep <0.8 and pct_tot_months_rep <0.8 then reportable_since=mdy(1,29,2017);
run;

data final_rep_2;
set final_rep_1;
if reportable_in_insights=1 then reportable_since=mdy(2,1,2015);
run;

data qc1(keep= geography_level geography_name reportable_since);
set final_rep_2;
run;
proc sql;
create table qc as
select reportable_since,count(*) as cnt from final_rep_1 group by reportable_since;
quit;

proc sql threads feedback;
create table reportability_metrics_city as
/*total retail*/
select 'City' as geography_level,city as geography_name,nrf_year,nrf_month,'Segment' as segcattype,'00' as segment_id
,count(distinct site_id) as distinct_sites,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1
group by 'City',city,nrf_year,nrf_month,'00','Segment'
union
/*segment*/
select 'City' as geography_level,city as geography_name,nrf_year,nrf_month,'Segment' as segcattype,put(segment_id,2.),count(distinct site_id) as distinct_sites,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1
group by 'City',city,nrf_year,nrf_month,segment_id,'Segment'
union
/*category*/
select 'City' as geography_level,city as geography_name,nrf_year,nrf_month,'Category' as segcattype,put(category_id,2.) as segment_id
,count(distinct site_id) as distinct_sites,count(distinct chain_id) as distinct_chains,
sum(case when comp_ind=1 then 1 else 0 end)/max(comp_days) as avg_daily_sites
from allinfo_bysite5 where comp_ind=1
group by 'City',city,nrf_year,nrf_month,category_id,'Category'
;
quit;
proc sql threads feedback;
create table chain_contrib_1_city as
/*total retail*/
select 'City' as geography_level,city as geography_name,nrf_year,nrf_month,chain_id,'Segment' as segcattype,'00' as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 group by 'City',city,nrf_year,nrf_month,chain_id,'00','Segment'
union
/*segment*/
select 'City' as geography_level,city as geography_name,nrf_year,nrf_month,chain_id,'Segment' as segcattype,put(segment_id,2.),sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 
/*where segment_id in (6,7)*/
group by 'City',city,nrf_year,nrf_month,chain_id,segment_id,'Segment'
union
/*category*/
select 'City' as geography_level,city as geography_name,nrf_year,nrf_month,chain_id,'Category' as segcattype,put(category_id,2.) as segment_id,sum(total_enters) as chain_enters,
count(distinct site_id) as chain_sites
from allinfo_bysite5 
/*where category_id in (4,5,20,22,23,24,25,45)*/
group by 'City',city,nrf_year,nrf_month,chain_id,category_id,'Category'
;
quit;

proc sql threads feedback;
create table chain_contrib2_city as
select geography_level,geography_name,nrf_year,nrf_month,segcattype,segment_id,max(chain_enters)/sum(chain_enters)
as chain_share,max(chain_sites)/sum(chain_sites) as chain_site_chare,sum(chain_sites) as total_sites,sum(chain_enters) as total_enters
from chain_contrib_1_city group by geography_level,geography_name,nrf_year,nrf_month,segcattype,segment_id
;
quit;

proc sql threads feedback;
create table chain_summary_city as 
select a.*,b.total_enters,b.total_sites,a.chain_enters/b.total_enters as pct_chain_share,a.chain_sites/b.total_sites as pct_chain_site_share
from chain_contrib_1_city a left join chain_contrib2_city b
on a.geography_level = b.geography_level and a.geography_name = b.geography_name and a.nrf_year = b.nrf_year and a.nrf_month = b.nrf_month
and a.segment_id = b.segment_id and a.segcattype = b.segcattype;
quit;

proc sql threads feedback;
create table final_rep_city as
select a.*,b.chain_share ,b.chain_site_chare as chain_site_share
from reportability_metrics_city a inner join chain_contrib2_city b
on a.nrf_month=b.nrf_month and a.nrf_year=b.nrf_year and a.geography_name=b.geography_name
and a.segment_id=b.segment_id and a.segcattype = b.segcattype
;
quit;

proc sql threads feedback;
create table stability_1_city as
select 'City' as geography_level,city as geography_name,nrf_year,nrf_month,date_enter,'Segment' as segcattype,'00' as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5
group by 'City',city,nrf_year,nrf_month,date_enter,'00','Segment'
union
/*segment*/
select 'City' as geography_level,city as geography_name,nrf_year,nrf_month,date_enter,'Segment' as segcattype,put(segment_id,2.),round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5
group by 'City',city,nrf_year,nrf_month,date_enter,segment_id,'Segment'
union
/*category*/
select 'City' as geography_level,city as geography_name,nrf_year,nrf_month,date_enter,'Category' as segcattype,put(category_id,2.) as segment_id,round((sum(total_enters)-sum(lag_enters))/sum(lag_enters),0.01) as comp_yoy
from allinfo_bysite5
group by 'City',city,nrf_year,nrf_month,date_enter,segment_id,'Category' 
;
quit;

data stability_1_city;
set stability_1_city ;
stable_yoy=(abs(comp_yoy)<=0.2);
run;
proc sql threads feedback;
create table stability_2_city as
select geography_level,geography_name,nrf_year,nrf_month,segcattype,segment_id,mean(stable_yoy) as pct_stable
from stability_1_city
group by geography_level,geography_name,nrf_year,nrf_month,segment_id,segcattype;
quit;
proc sql threads feedback;
create table final_rep1_city as
select a.*,b.pct_stable
from final_rep_city a inner join stability_2_city b
on a.nrf_month=b.nrf_month and a.nrf_year=b.nrf_year and a.geography_name=b.geography_name and a.geography_level = b.geography_level
and a.segment_id=b.segment_id and a.segcattype = b.segcattype
;
quit;
data final_rep1_city;
set final_rep1_city;
multiple_chains=(distinct_chains>=3 & distinct_chains^=.);
sufficient_sites=(avg_daily_sites>=30 & avg_daily_sites^=.);
chain_share_flag=(chain_share<=0.5 & chain_share^=.);
chain_site_share_flag=(chain_site_share<=0.5 & chain_site_share^=.);
stability=(pct_stable>=0.8 & pct_stable^=.);
reportable=(multiple_chains=1 & sufficient_sites=1 & chain_share_flag=1 & chain_site_share_flag=1 & stability=1);
reportability_pct=(multiple_chains+sufficient_sites+chain_share_flag+chain_site_share_flag+stability)/5;
run;
proc sql threads feedback;
create table final_rep2_city as 
select a.*,b.SegmentName,c.market_name from final_rep1_city a left join segment_name_mapping b
on strip(a.segment_id)=strip(b.SegmentID) and strip(a.segcattype)=strip(b.SegCattype)
left join market_names c on strip(a.geography_name) = strip(c.market_id);
quit;
data final_rep2_city;
set final_rep2_city;
drop geography_name;
rename market_name = geography_name;
where geography_name ^='.' and geography_name^='';
run;

proc sql threads feedback;
create table final_rep_4 as
select a.*,b.month_num as nrf_month_num from final_rep1_city a left join nrf_month_num b on a.nrf_month=b.month;
quit;
proc sql threads feedback;
create table region_names as
select geography_level,geography_name,count(*) as count from final_rep_4 
group by geography_level,geography_name;
quit;
data region_names;
set region_names;
drop count;
run;

proc sql threads feedback;
create table nrf_years as
select distinct nrf_year from final_rep_4 where nrf_year ^=.;
quit;
proc sql threads feedback;
create table nrf_months as
select nrf_month,nrf_month_num,count(*) as count from final_rep_4 where nrf_month ^=''
group by nrf_month,nrf_month_num;
quit;
data nrf_months;
set nrf_months;
drop count;
run;
proc sql threads feedback;
create table segtype  as
select distinct segcattype from final_rep_4;
quit;
proc sql threads feedback;
create table segs as
select segcattype,segment_id,count(*) as count from final_rep_4 where segment_id ^='.' group by segcattype,segment_id;
quit;
data segs;
set segs;
drop count;
run;

proc sql threads feedback;
create table us_rep_sheet_skeleton as
select * From
	region_names , nrf_years,nrf_months,segs;
quit;
data temp1;
set us_rep_sheet_skeleton;
where (nrf_year=2018 and nrf_month_num<=11);
run;
data temp2;
set us_rep_sheet_skeleton;
where nrf_year^=2018;
run;
data us_rep_skeleton;
set temp2 temp1;
run;
proc sql threads feedback;
create table us_rep_sheet as
select a.geography_level,a.geography_name,a.nrf_year,a.nrf_month,a.nrf_month_num,a.segcattype,a.segment_id,b.* From
us_rep_skeleton a left join final_rep_4 b on 
strip(a.geography_level)=strip(b.geography_level) and
strip(a.geography_name)=strip(b.geography_name) and 
a.nrf_year=b.nrf_year and
a.nrf_month=b.nrf_month and
a.nrf_month_num=b.nrf_month_num and 
a.segcattype=b.segcattype and 
a.segment_id=b.segment_id and a.segment_id^=" ";
quit;
proc sql threads feedback;
create table temp as
select geography_level,geography_name,nrf_year,nrf_month,nrf_month_num,segcattype,segment_id,count(*) from us_rep_sheet
group by geography_level,geography_name,nrf_year,nrf_month,nrf_month_num,segcattype,segment_id having count(*)>1;
quit;
proc sort data=us_rep_sheet;
by geography_level geography_name segcattype segment_id  nrf_year nrf_month_num ;
run;
/*data us_rep_sheet;*/
/*set us_rep_sheet;*/
/*where segcattype^="";*/
/*run;*/
%let n = 7;
data us_rep_sheet;
set us_rep_sheet;
format concat $50.;
concat=catx('-',geography_name,segment_id,segcattype);
run;
proc sort data=us_rep_sheet;
by geography_level concat nrf_year nrf_month_num ;
run;
data us_rep_sheet_1;
  set us_rep_sheet;
  by geography_level concat nrf_year nrf_month_num;
  retain rep_sum 0;
  if first.concat then do;
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

data us_rep_sheet_1;
set us_rep_sheet_1;
format final_reportability best12.;
final_reportability=reportable;
if Past6MonthsRep=. then Past6MonthsRep=reportable;
if reportable=0 then final_reportability=(Past6MonthsRep>=0.8 & reportability_pct>=0.8);
drop rep_sum count last7;
where geography_name^= '';
run;

