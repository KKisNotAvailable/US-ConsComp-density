# US-ConsComp-density

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
#### need more data
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
  4. [CLEANING] vacancy rate
     https://www.census.gov/housing/hvs/data/prevann.html
     裡面有homeowner vac, gross vac, year-round vac (4~5a), def見第一PDF
     >> Table 5a.
  5. [???] median number of days property listings spend on the market (類似周轉率)
     https://www.realtor.com/research/data/ => monthly inventory => county, historical data (2016.07-)
  6. [DONE] median household income (to measure affordability)
     https://fred.stlouisfed.org/searchresults?st=county+level+median+household+income
     >> county (06037): https://fred.stlouisfed.org/series/MHICA06037A052NCEN
  7. [CLEANING] employment rate
     https://www.bls.gov/lau/data.htm
     >> county (48039): https://data.bls.gov/dataViewer/view/timeseries/LAUCN480390000000004
     >> state (06): https://data.bls.gov/dataViewer/view/timeseries/LASST060000000000003
  8. [???] 一些供給面的 (eg. construction cost, Wharton Residential Land Use Regulation Index, and land availability index?)
     >> construction: https://fred.stlouisfed.org/tags/series?t=construction%3Bprice+index => [NotThis]
      => does transportation for construction index ?? exist?
     >> WRLURI: https://real-faculty.wharton.upenn.edu/gyourko/land-use-survey/ (2006 & 2018)
  9. [CLEANING] weather condition (有無自然災害影響 => should be some yearly binary code per county?)
     >> https://www.fema.gov/data-visualization/disaster-declarations-states-and-counties
> construction company characteristics (from compustat)

#### Filter conditions of CoreLogic
1.

### [Plot] Company-specific responses through house price cycles
(幾乎都是好壞時期的比較)
1. properties sold during good/bad time for small and large companies
2. properties built ...


Q會分大小，price看整個county，不分年份

用有合到compustat的前後1/3，

** No need to use compustat for now...