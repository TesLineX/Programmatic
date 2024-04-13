import requests as rq, pandas as pd
pd.options.mode.chained_assignment = None
startDate = '2024-04-01'
endDate = '2024-04-30'
dsp_id = '12482'
dsp_part_id = '45649'
token = ''


def link_1(start_date, end_date):
  text = f'https://daopush-api.info/api/statistic/pops-web?api-key=ms4lZG0x86s2RymbEs415YqFhbUTRpve&groupBy=days&sourceId={dsp_part_id}&startDate={start_date}&endDate={end_date}&timeZone=UTC'
  return text


url_1 = link_1(startDate , endDate)
response = rq.get(url_1)
data_1 = response.json()
df_dsp = pd.DataFrame(data_1['items'])

for i in range(len(df_dsp['period'])):
  df_dsp['period'][i] = df_dsp['period'][i][:10]


def link_2(date_from, date_to, dsp_id):
  text = f'https://backapi.admy.com/api/v1/reports/dsp/bydate?token={token}&dsp_id={dsp_id}&date_from={date_from}&date_to={date_to}'
  return text


url_2 = link_2(startDate, endDate, dsp_id)
response = rq.get(url_2)
data_2 = response.json()
df_rp = pd.DataFrame(data_2['data'])
df_join = df_dsp.merge(df_rp, how='inner', left_on='period', right_on='date', suffixes=('_x', '_y'))
df_join.drop(['uniqs', 'hits', 'requests_x', 'requests_y', 'responses', 'win_responses', 'currency', 'period'], axis = 1, inplace = True)
df_join.rename(columns = {'date': 'Date'}, inplace = True)
df_join.rename(columns = {'clicks_x': 'DaoPush Clicks'}, inplace = True)
df_join.rename(columns = {'pay': 'DaoPush Revenue'}, inplace = True)
df_join.rename(columns = {'clicks_y': 'RealPush Clicks'}, inplace = True)
df_join.rename(columns = {'amount': 'RealPush Revenue'}, inplace = True)
df_join.rename(columns = {'publisher_amount': 'Publisher Revenue'}, inplace = True)
df_join.fillna(0, inplace = True)
df_join['CLK DSC %'] = ((1 - (1 / df_join['RealPush Clicks'] * df_join['DaoPush Clicks'])) * 100)
df_join['REV DSC %'] = ((1 - (1 / df_join['RealPush Revenue'] * df_join['DaoPush Revenue'])) * 100)
df_join['DaoPush Revenue'] = df_join['DaoPush Revenue'].round(2)
df_join['RealPush Revenue'] = df_join['RealPush Revenue'].round(2)
df_join['Publisher Revenue'] = df_join['Publisher Revenue'].round(2)
df_join['PROFIT'] = (df_join['DaoPush Revenue'] - df_join['Publisher Revenue']).round(2)
df_join.fillna(0, inplace = True)
df_join.loc[len(df_join.index)] = [df_join['DaoPush Revenue'].sum(),
                                   df_join['DaoPush Clicks'].sum(),
                                   'Total',
                                   '---',
                                   df_join['RealPush Clicks'].sum(),
                                   df_join['RealPush Revenue'].sum(),
                                   df_join['Publisher Revenue'].sum(),
                                   ((1 - (1 / df_join['RealPush Clicks'].sum() * df_join['DaoPush Clicks'].sum())) * 100),
                                   ((1 - (1 / df_join['RealPush Revenue'].sum() * df_join['DaoPush Revenue'].sum())) * 100),
                                   df_join['PROFIT'].sum()
                                   ]
df_join['CLK DSC %'] = df_join['CLK DSC %'].round(1)
df_join['REV DSC %'] = df_join['REV DSC %'].round(1)
df_join['RealPush Clicks'] = df_join.apply(lambda x: "{:,d}".format(x['RealPush Clicks']).replace(',', ' '), axis=1)
df_join['DaoPush Clicks'] = df_join.apply(lambda x: "{:,d}".format(x['DaoPush Clicks']).replace(',', ' '), axis=1)
df_join['SHARE'] = '-'
for i in range(len(df_join)):
  if df_join['RealPush Clicks'].iloc[i] != '0':
    a = ((1 / df_join['DaoPush Revenue'][i] * df_join['Publisher Revenue'][i]) * 100).round(0)
    b = ((1 / df_join['DaoPush Revenue'][i] * df_join['PROFIT'][i]) * 100).round(0)
    a, b = int(a), int(b)
    df_join['SHARE'][i] = str(a) + ' <> ' + str(b)
  else:
    continue
df_join = df_join[['Date', 'dsp_id', 'DaoPush Clicks', 'RealPush Clicks', 'DaoPush Revenue', 'RealPush Revenue', 'Publisher Revenue', 'PROFIT','SHARE', 'CLK DSC %', 'REV DSC %']]
df_join

