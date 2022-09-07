
/*Creating user defined format*/

proc format;
value $hb 
"Ayrshire and Arran" = "NHS Ayrshire and Arran"

"Borders"="NHS Borders"

"Dumfries and Galloway"="NHS Dumfries and Galloway"

"Fife"="NHS Fife"

"Forth Valley"="NHS Forth Valley"

"Grampian"="NHS Grampian"

"Greater Glasgow and Clyde"="NHS Greater Glasgow and Clyde"

"Highland"="NHS Highland"

"Lanarkshire"="NHS Lanarkshire"

"Lothian"="NHS Lothian"

"Orkney"="NHS Orkney"

"Shetland"="NHS Shetland"

"Tayside"="NHS Tayside"

"Western Isles"="NHS Western Isles";
run;

/*Importing RNA dataset*/

PROC IMPORT datafile="/users/mycles/dissertation_data/RNA_data.xlsx"
  out=rna_ dbms=xlsx replace;
  getnames=no;
  Datarow=2;
RUN;
proc sort data=rna_;
by c;
run;

data rna;
set rna_;
length HBName $29.;
format HBName $hb.;
SiteName=B;
HBName=A;
Population=E;
rna_value=G;
rna_value_unweighted=rna_value;
datevar = input(C,32.);
format datevar date9.;
if HBName ne "(Empty)";
run;

PROC SORT DATA=rna(drop=SiteName A B C D E F G H);
BY datevar HBName;
RUN;
DATA rna1;
set rna;
BY datevar HBName;
p=vtype(Population);
pop=input(Population,8.);
p1=vtype(pop);
RUN;
proc means data= rna1;
    by datevar HBName;
    var rna_value/weight=pop;
    output out = w_rna;
run;

DATA rna_fin(drop= _STAT_ _TYPE_ _FREQ_);
SET w_rna;
If not missing(datevar) then datevar1=strip(put(datevar,yymmdd10.));
HB1=strip(upcase(put(HBName,$hb.)));
IF _STAT_="MEAN";
RUN;

proc sort data=rna_fin;
by datevar1 HB1;
run;

/*Importing Covid-19 cases data*/

PROC IMPORT datafile="/users/mycles/dissertation_data/trend_hb_20220629.csv"
  out=hb_daily dbms=csv replace;
  getnames=yes;
  Datarow=2;
RUN;

proc sort data=hb_daily;
by Date;
run;

data hb_daily_cases(KEEP= HBName Date HB CumulativePositive DailyPositive datevar1 HB1/*DailyDeaths CumulativeDeaths 
CrudeRateDeaths PositiveTests TotalTests PositivePillar1 PositivePillar2 FirstInfections FirstInfectionsCumulative 
Reinfections ReinfectionsCumulative PercentReinfections TotalPillar1 TotalPillar2*/ datevar);
set hb_daily;
IF HBName not in("Scotland","Golden Jubilee National Hospi");
HB1=strip(upcase(HBName));
datevar=input(put(Date,8.),yymmdd8.);
format datevar date9.;
If not missing(Date) then datevar1=strip(put(input(put(Date,8.),yymmdd8.),yymmdd10.));
run;

data hb_daily_cases1;
set hb_daily_cases;
If '08aug2020'd le datevar le '20apr2022'd ;
RUN;

proc sort data=hb_daily_cases1;
by datevar1 HB1;
run;
proc sql;
create table new as select a.*,b.rna_value
from hb_daily_cases1 as a left join rna_fin as b
on a.datevar1=b.datevar1 and a.HB1=b.HB1
order by datevar1, HBName;
RUN;

PROC SORT DATA=new;
BY HB1 datevar1;
RUN;


data ret;
retain l_rna;
set new;
BY HB1 datevar1;
if not missing(rna_value) then l_rna=rna_value;
else rna_value=l_rna;
drop l_rna;
run;


proc sort data=ret;
by datevar;
run;



data fin;
set ret;
weekday=weekday(datevar);
if weekday=1 then day="Sun";
else if weekday=2 then day="Mon";
else if weekday=3 then day="Tue";
else if weekday=4 then day="Wed";
else if weekday=5 then day="Thu";
else if weekday=6 then day="Fri";
else if weekday=7 then day="Sat";
else day="N\A";
run;

/*proc sql;*/
/*create table fin1 as select distinct datevar, sum(rna_value) as tot_rna, sum(DailyPositive) as tot_pos from fin */
/*group by datevar */
/*order by datevar;*/
/*QUIT;*/


/*take from 08-08-2020 to 2022-04-20*/

Data final(DROP=Date HB1 HB datevar1);
set fin;
/*when HBName="NHS Ayrshire and Arran" all the other variables have value 0 */
if HBName="NHS Borders" then NHS_Borders=1;
else NHS_Borders=0;
if HBName="NHS Dumfries and Galloway" then NHS_Dumfries_and_Galloway=1;
else NHS_Dumfries_and_Galloway=0;
if HBName="NHS Fife" then NHS_Fife=1;
else NHS_Fife=0;
if HBName="NHS Forth Valley" then NHS_Forth_Valley=1;
else NHS_Forth_Valley=0;
if HBName="NHS Grampian" then NHS_Grampian=1;
else NHS_Grampian=0;
if HBName="NHS Greater Glasgow and Clyde" then NHS_Greater_Glasgow_and_Clyde=1;
else NHS_Greater_Glasgow_and_Clyde=0;
if HBName="NHS Highland" then NHS_Highland=1;
else NHS_Highland=0;
if HBName="NHS Lanarkshire" then NHS_Lanarkshire=1;
else NHS_Lanarkshire=0;
if HBName="NHS Lothian" then NHS_Lothian=1;
else NHS_Lothian=0;
if HBName="NHS Orkney" then NHS_Orkney=1;
else NHS_Orkney=0;
if HBName="NHS Shetland" then NHS_Shetland=1;
else NHS_Shetland=0;
if HBName="NHS Tayside" then NHS_Tayside=1;
else NHS_Tayside=0;
if HBName="NHS Western Isles" then NHS_Western_Isles=1;
else NHS_Western_Isles=0;
if not cmiss(rna_value,DailyPositive);
run;
proc sort data=final;
by datevar;
run;

proc export data=final
    outfile="/users/mycles/dissertation_data/final1.xlsx"
    dbms=xlsx
    replace;
run;
