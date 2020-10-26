# Big_data: Parking Violations in NYC (Spark)

In this challenge, we would like to gather statistics on the number of parking violations (tickets) per street
segment in NYC over the past 5 years. In particular, for each street segment in NYC, we would like to have the
following:
1. The total number of parking violations for each year from 2015 to 2019.
2. The rate that the total number of violations change over the years using Ordinary Least Squares.

INPUT DATA:
Parking Violations Issued – Fiscal Year 2015-2019 (around 10GB in total)
Source: https://data.cityofnewyork.us/browse?q=%22Parking%20Violations%20Issued%22
Description: Data sets contain parking violations issued during the respective fiscal year. More info
(including data dictionary) can be found on one of the results listed in the above link. All 5-year data

NYC Street Centerline (CSCL) (around 120k segments but our version has almost 650k)
Source: https://data.cityofnewyork.us/City-Government/NYC-Street-Centerline-CSCL-/exjm-f27b
Description: The NYC Street Centerline (CSCL) is a road-bed representation of New York City streets
containing address ranges and other information such as traffic directions, road types, segment types.
Details on this data set are available at: https://github.com/CityOfNewYork/nyc-geometadata/blob/master/Metadata/Metadata_StreetCenterline.md
