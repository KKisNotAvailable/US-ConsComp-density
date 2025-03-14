# US-Homebuilder-Concentration

## Introduction
This is the repo for paper "Homebuilder concentration and housing market dynamics".

## Development Log
1. We originally plan to merge Deed and Tax files on CLIP, and for those entries without CLIP, we will use APN. However, after a second thought, we will first fill the missing CLIPs for those entries with only APN, and the merge the files. Here is why. Consider the following case:
|Deed|CLIP|APN|Tax|CLIP|APN|
|-|-|-|-|-|-|
||123|ABC||123|ABC|
||-|ABC||-|ABC|
The first record would be merged using CLIP and the second record would use APN as key to merge, and the result would be
|Merged|CLIP|APN|
|-|-|-|
||123|ABC|
||-|ABC|
The problem is, when we merge Deed with Tax, we need to find the tax data with the assessment year closest to the transaction year in deed data. In our example, although the two records are both indicating the same property (since they have the same APN), their assessment years were not compared before merging. Therefore, for deed, the first record did not consider the second record in tax, because the tax data does not have CLIP. Similarly, the second record in deed did not consider the first record in tax, because this deed record does not have a CLIP.

As a result, we need to fill CLIPs to those records having only APN in both Deed and Tax. In "preprocess_2023.py," CheckData.clip_apn_bijection() is used to check if CLIP and APN are actually bijective. Then this filling operations are in Preprocess.single_deed_clean, single_tax_clean, and hist_tax_clean.

<!-- Deed only need to fill the CLIP, but Tax need to fill both CLIP and APN -->

2. <!-- This part is not done--> In the historical tax files, from the file names, we can know the approximate assessment year. For example, FIPS_01001_duke_university_hist_property_basic1_dpc_01465913_02_20230803_084057_data, the "02" before "20230803" is the serial number for this file. "02" means the assessment year is 2020. We mentioned this is "approximate" is because...
Also, we found that in the tax files, "TAX YEAR" and "ASSESS YEAR" represent the same thing, while sometimes one of them might be missing values.

Although in the tax files, there are SALE DATE, there's no such columns in the historical tax file. Plus, we think using the closest assess year should also reflect the property's status on transaction, we align TAX and HIST TAX and use ASSESS YEAR to merge with DEED.

3. There are no "UNIVERSAL BUILDING SQUARE FEET SOURCE INDICATOR CODE" in historical tax.

<!--
## Empirical Analysis
### Descriptive Analysis
#### Numerical
The definition of construction company: the seller column, if the column 'RESALE/NEW_CONSTRUCTION' == 'N', meaning this is a new house sale, this seller should be a construction company.

There are about ??? distinctive construction companies
> TODO: how to check if the companies are distinctive, some record might be slightly different when marking the name of the company...

The original data has ??? rows, in the period of 20xx ~ 2016. The filtered data has a size of ??? rows, with the following filtering conditions:
1. ...

#### Plots
1. heat map of ENI (with a colorbar)
2. scatter plot of price change and ENI
3. time series of house price of new constructions, along with the mean price of all real estate transactions.

https://www.zillow.com/research/data/
這個有Metro的sale count, sale amount (2008-), days on market (2018-)
sale cnt, amt 也有for new construction的 (2018-)

https://data.census.gov/table
可以去這裡挖個寶, 但目前看起來只有存近10年, archive不知道在哪

https://www.census.gov/topics/housing.html
稍微看了看沒有目前缺的資料

### Regression Analysis
city to county FIPS mapping: https://www.census.gov/library/reference/code-lists/ansi.html#cousub
#### Variables
> county-level info
  1. [DONE] yearly population
     https://www.census.gov/data/tables/time-series/demo/popest/2020s-counties-total.html 下面的archive: Dataset
     >> 2000-2009: 2000-2009/counties/totals/co-est2009-alldata.csv
     >> 2010-2019: 2010-2019/counties/totals/co-est2019-alldata.csv
     >> 2020-2023: 2020-2023/counties/totals/co-est2023-alldata.csv
  2. [CORELOGIC] number / dollar amount of properties sold (both first and second hand)
     not down to even state level: https://www.census.gov/construction/nrs/data/series.html
     >> will use data directly from CoreLogic
  3. [DONE] housing stock
     https://www.census.gov/data/tables/time-series/demo/popest/2020s-total-housing-units.html 下面的archive: Dataset (會進到跟上面population一樣的地方)
     >> 2000-2009: 2000-2009/housing/totals/hu-est2009-us.csv
     >> 2010-2020: 2010-2020/housing/HU-EST2020_ALL.csv
     >> 2020-2023: 中間一堆state的地方按第一個 United States: CO-EST2023-HU.xlsx
  4. [DONE] vacancy rate
     https://www.census.gov/housing/hvs/data/prevann.html
     裡面有homeowner vac, gross vac, year-round vac (4~5a), def見第一PDF
     >> Table 5a. (2005-)
  5. [???] median number of days property listings spend on the market (類似周轉率)
     https://www.realtor.com/research/data/ => monthly inventory => county, historical data (2016.07-)
  6. [DONE] median household income (to measure affordability)
     https://fred.stlouisfed.org/searchresults?st=county+level+median+household+income
     >> county (06037): https://fred.stlouisfed.org/series/MHICA06037A052NCEN
  7. [DONE] unemployment rate
     https://www.bls.gov/lau/data.htm
     >> county (48047): https://data.bls.gov/dataViewer/view/timeseries/LAUCN480470000000003#

     unemployment
     >> county (48039): https://data.bls.gov/dataViewer/view/timeseries/LAUCN480390000000004
     >> state (06): https://data.bls.gov/dataViewer/view/timeseries/LASST060000000000003

  8. [???] 一些供給面的 (eg. construction cost, Wharton Residential Land Use Regulation Index, and land availability index?)
     >> construction: https://fred.stlouisfed.org/tags/series?t=construction%3Bprice+index => [NotThis]
      => does transportation for construction index ?? exist?
     >> WRLURI: https://real-faculty.wharton.upenn.edu/gyourko/land-use-survey/ (2006 & 2018)
  9. [DONE] weather condition (有無自然災害影響 => should be some yearly binary code per county?)
     >> https://www.fema.gov/data-visualization/disaster-declarations-states-and-counties
> construction company characteristics (from compustat)

   10. HPI
      >> https://www.fhfa.gov/data/hpi/datasets?tab=hpi-datasets

#### Filter conditions of CoreLogic
1.

# TODO
1. Align where the code calls data, it's chaos.
2. Avoid commenting sections out just for producing certain file, find some way to design this.

### [Plot] Company-specific responses through house price cycles
(幾乎都是好壞時期的比較)
1. properties sold during good/bad time for small and large companies
2. properties built ...

## NOTES
* In corelogic_seller_panel.csv, the median unit price is not the same as in the county-aggregated median unit price. In seller panel, this is really the median unit price of all properties this seller sold. But in county-aggregation, the median is actually the median of all seller's mean unit price (not median unit price) in that county.
* In processed_data, corelogic_panel_20pct_v1, 20pct means the top was defined as top 20% (bot same logic); v1 means no year 2000, no median calculated; v2 means no year 2000 but median included; v3 means year 2000 and median both included.
-->