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

### Regression Analysis
#### need more data
> county-level info
  1. yearly population
  2. number / dollar amount of properties sold (both first and second hand)
  3. housing stock
  4. vacancy rate
  5. median number of days property listings spend on the market (類似周轉率)
  6. median household income (to measure affordability)
  7. employment rate
  8. 一些供給面的 (eg. construction cost, Wharton Residential Land Use Regulation Index, and land availability index?)
  9. weather condition (有無自然災害影響 => should be some yearly binary code per county?)
> construction company characteristics (from compustat)

### [Plot] Company-specific responses through house price cycles
(幾乎都是好壞時期的比較)
1. properties sold during good/bad time for small and large companies
2. properties built ...
