%macro regionwise(region,HB,num);

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
SiteName=B;
HBName=A;
Population=E;
rna_value=G;
rna_value_unweighted=rna_value;
datevar = input(C,32.);
format datevar date9.;
if HBName=&region;
run;

PROC SORT DATA=rna(drop=SiteName A B C D E F G H);
BY datevar;
RUN;
DATA rna1;
set rna;
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
IF _STAT_="MEAN";
RUN;

proc sort data=rna_fin;
by datevar;
run;

PROC IMPORT datafile="/users/mycles/dissertation_data/trend_hb_20220629.csv"
  out=hb_daily dbms=csv replace;
  getnames=yes;
  Datarow=2;
RUN;

proc sort data=hb_daily;
by Date;
run;

data hb_daily_cases(KEEP= HBName Date HB CumulativePositive DailyPositive DailyDeaths CumulativeDeaths 
CrudeRateDeaths PositiveTests TotalTests PositivePillar1 PositivePillar2 FirstInfections FirstInfectionsCumulative 
Reinfections ReinfectionsCumulative PercentReinfections TotalPillar1 TotalPillar2 datevar);
set hb_daily;
IF strip(HBName)=&HB;
datevar=input(put(Date,8.),yymmdd8.);
format datevar date9.;
run;

data hb_daily_cases1;
set hb_daily_cases;
If datevar > '27may2020'd;
RUN;

PROC SORT DATA=hb_daily_cases1(DROP= HB Date);
BY datevar;
RUN;

data rna_site;
merge rna_fin hb_daily_cases1(IN=A);
by datevar;
IF A;
run;

data rna_site_final;
set rna_site;
retain _rna_value;
if not missing(rna_value) then _rna_value=rna_value;
else rna_value=_rna_value;
drop _rna_value;
run;

proc sort data=rna_site_final;
by datevar;
run;



data _&num;
set rna_site_final;
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

%mend;

/*Regionwise data*/

%regionwise("Ayrshire and Arran","NHS Ayrshire and Arran",1);

%regionwise("Borders","NHS Borders",2);

%regionwise("Dumfries and Galloway","NHS Dumfries and Galloway",3);

%regionwise("Fife","NHS Fife",4);

%regionwise("Forth Valley","NHS Forth Valley",5);

%regionwise("Grampian","NHS Grampian",6);

%regionwise("Greater Glasgow and Clyde","NHS Greater Glasgow and Clyde",7);

%regionwise("Highland","NHS Highland",8);

%regionwise("Lanarkshire","NHS Lanarkshire",9);

%regionwise("Lothian","NHS Lothian",10);

%regionwise("Orkney","NHS Orkney",11);

%regionwise("Shetland","NHS Shetland",12);

%regionwise("Tayside","NHS Tayside",13);

%regionwise("Western Isles","NHS Western Isles",14);


Data fin;
SET _1 _2 _3 _4 _5 _6 _7 _8 _9 _10 _11 _12 _13 _14;
run;


Data final;
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
