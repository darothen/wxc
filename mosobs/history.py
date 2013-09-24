import pandas
import itertools

xls = pandas.ExcelFile("pensacola.xlsx")

forecasts = xls.parse('FORECAST', skiprows=[0], parse_cols=[0,1,2,3,4], index_col=0)
obs = xls.parse('FORECAST', skiprows=[0], parse_cols=[0,5,6,7,8], index_col=0)

## Assembling MultiIndex-ed DataFrame
data = pandas.concat([forecasts, obs], axis=1, keys=['FORS', 'OBS'])