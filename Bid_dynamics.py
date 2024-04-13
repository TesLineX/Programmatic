import requests as rq, pandas as pd, time, matplotlib.pyplot as plt, slack_sdk, schedule, matplotlib
from datetime import timedelta, datetime
pd.options.mode.chained_assignment = None
matplotlib.use('Agg')
token = ''
slack_token = ''
slack_channel = ''

def job():
  print('START BID DYNAMICS ', datetime.now())
  now = datetime.now()
  date_from = now - timedelta(days = 7)
  time_format = '%Y-%m-%d'
  #API Token пользователя. Обязательный параметр

  #Тип трафика. Значения: 0 - pop, 2 - push, 3 - native, 8 - inpagepush
  traffic_type = ['0', '2', '3', '8']
  countries = {'AD':'Andorra','AE':'United Arab Emirates (the)','AF':'Afghanistan','AG':'Antigua and Barbuda','AI':'Anguilla','AL':'Albania','AM':'Armenia','AO':'Angola',
  'AQ':'Antarctica','AR':'Argentina','AS':'American Samoa','AT':'Austria','AU':'Australia','AW':'Aruba','AX':'Åland Islands','AZ':'Azerbaijan','BA':'Bosnia and Herzegovina','BB':'Barbados',
  'BD':'Bangladesh','BE':'Belgium','BF':'Burkina Faso','BG':'Bulgaria','BH':'Bahrain','BI':'Burundi','BJ':'Benin','BL':'Saint Barthélemy','BM':'Bermuda','BN':'Brunei Darussalam',
  'BO':'Bolivia (Plurinational State of)','BQ':'Bonaire, Sint Eustatius and Saba','BR':'Brazil','BS':'Bahamas (the)','BT':'Bhutan','BV':'Bouvet Island','BW':'Botswana','BY':'Belarus',
  'BZ':'Belize','CA':'Canada','CC':'Cocos (Keeling) Islands (the)','CD':'Congo (the Democratic Republic of the)','CF':'Central African Republic (the)','CG':'Congo (the)',
  'CH':'Switzerland','CI':"'Côte d'Ivoire",'CK':'Cook Islands (the)','CL':'Chile','CM':'Cameroon','CN':'China','CO':'Colombia','CR':'Costa Rica','CU':'Cuba','CV':'Cabo Verde',
  'CW':'Curaçao','CX':'Christmas Island','CY':'Cyprus','CZ':'Czechia','DE':'Germany','DJ':'Djibouti','DK':'Denmark','DM':'Dominica','DO':'Dominican Republic (the)','DZ':'Algeria',
  'EC':'Ecuador','EE':'Estonia','EG':'Egypt','EH':'Western Sahara','ER':'Eritrea','ES':'Spain','ET':'Ethiopia','FI':'Finland','FJ':'Fiji','FK':'Falkland Islands (the) [Malvinas]',
  'FM':'Micronesia (Federated States of)','FO':'Faroe Islands (the)','FR':'France','GA':'Gabon','GB':'United Kingdom of Great Britain and Northern Ireland (the)','GD':'Grenada',
  'GE':'Georgia','GF':'French Guiana','GG':'Guernsey','GH':'Ghana','GI':'Gibraltar','GL':'Greenland','GM':'Gambia (the)','GN':'Guinea','GP':'Guadeloupe','GQ':'Equatorial Guinea',
  'GR':'Greece','GS':'South Georgia and the South Sandwich Islands','GT':'Guatemala','GU':'Guam','GW':'Guinea-Bissau','GY':'Guyana','HK':'Hong Kong','HM':'Heard Island and McDonald Islands',
  'HN':'Honduras','HR':'Croatia','HT':'Haiti','HU':'Hungary','ID':'Indonesia','IE':'Ireland','IL':'Israel','IM':'Isle of Man','IN':'India','IO':'British Indian Ocean Territory (the)',
  'IQ':'Iraq','IR':'Iran (Islamic Republic of)','IS':'Iceland','IT':'Italy','JE':'Jersey','JM':'Jamaica','JO':'Jordan','JP':'Japan','KE':'Kenya','KG':'Kyrgyzstan',
  'KH':'Cambodia','KI':'Kiribati','KM':'Comoros (the)','KN':'Saint Kitts and Nevis','KP':"Korea (the Democratic People's Republic of)",'KR':'Korea (the Republic of)','KW':'Kuwait',
  'KY':'Cayman Islands (the)','KZ':'Kazakhstan','LA':"Lao People's Democratic Republic (the)",'LB':'Lebanon','LC':'Saint Lucia','LI':'Liechtenstein','LK':'Sri Lanka','LR':'Liberia','LS':'Lesotho',
  'LT':'Lithuania','LU':'Luxembourg','LV':'Latvia','LY':'Libya','MA':'Morocco','MC':'Monaco','MD':'Moldova (the Republic of)','ME':'Montenegro','MF':'Saint Martin (French part)','MG':'Madagascar',
  'MH':'Marshall Islands (the)','MK':'Republic of North Macedonia','ML':'Mali','MM':'Myanmar','MN':'Mongolia','MO':'Macao','MP':'Northern Mariana Islands (the)','MQ':'Martinique','MR':'Mauritania',
  'MS':'Montserrat','MT':'Malta','MU':'Mauritius','MV':'Maldives','MW':'Malawi','MX':'Mexico','MY':'Malaysia','MZ':'Mozambique','NA':'Namibia','NC':'New Caledonia','NE':'Niger (the)',
  'NF':'Norfolk Island','NG':'Nigeria','NI':'Nicaragua','NL':'Netherlands (the)','NO':'Norway','NP':'Nepal','NR':'Nauru','NU':'Niue','NZ':'New Zealand','OM':'Oman','PA':'Panama','PE':'Peru',
  'PF':'French Polynesia','PG':'Papua New Guinea','PH':'Philippines (the)','PK':'Pakistan','PL':'Poland','PM':'Saint Pierre and Miquelon','PN':'Pitcairn','PR':'Puerto Rico',
  'PS':'Palestine, State of','PT':'Portugal','PW':'Palau','PY':'Paraguay','QA':'Qatar','RE':'Réunion','RO':'Romania','RS':'Serbia','RU':'Russian Federation (the)','RW':'Rwanda',
  'SA':'Saudi Arabia','SB':'Solomon Islands','SC':'Seychelles','SD':'Sudan (the)','SE':'Sweden','SG':'Singapore','SH':'Saint Helena, Ascension and Tristan da Cunha','SI':'Slovenia',
  'SJ':'Svalbard and Jan Mayen','SK':'Slovakia','SL':'Sierra Leone','SM':'San Marino','SN':'Senegal','SO':'Somalia','SR':'Suriname','SS':'South Sudan','ST':'Sao Tome and Principe',
  'SV':'El Salvador','SX':'Sint Maarten (Dutch part)','SY':'Syrian Arab Republic','SZ':'Eswatini','TC':'Turks and Caicos Islands (the)','TD':'Chad','TF':'French Southern Territories (the)',
  'TG':'Togo','TH':'Thailand','TJ':'Tajikistan','TK':'Tokelau','TL':'Timor-Leste','TM':'Turkmenistan','TN':'Tunisia','TO':'Tonga','TR':'Turkey','TT':'Trinidad and Tobago','TV':'Tuvalu',
  'TW':'Taiwan (Province of China)','TZ':'Tanzania, United Republic of','UA':'Ukraine','UG':'Uganda','UM':'United States Minor Outlying Islands (the)','US':'United States of America (the)',
  'UY':'Uruguay','UZ':'Uzbekistan','VA':'Holy See (the)','VC':'Saint Vincent and the Grenadines','VE':'Venezuela (Bolivarian Republic of)','VG':'Virgin Islands (British)',
  'VI':'Virgin Islands (U.S.)','VN':'Viet Nam','VU':'Vanuatu','WF':'Wallis and Futuna','WS':'Samoa','YE':'Yemen','YT':'Mayotte','ZA':'South Africa','ZM':'Zambia','ZW':'Zimbabwe'}

  def link(date_from, date_to, traffic_type):
    text = f'https://backapi.admy.com/api/v1/reports/dsp/bycountries?token={token}&date_from={date_from}&date_to={date_to}&traffic_type={traffic_type}'
    return text

  for t_t in range(len(traffic_type)):
    df_add = pd.DataFrame()
    for j in range(7):
      d_t = date_from + timedelta(days=j)
      d_t = d_t.strftime(time_format)
      url = link(d_t, d_t, traffic_type[t_t])
      response = rq.get(url)
      data = response.json()
      df_temp = pd.DataFrame(data['data'])
      df_temp = df_temp.groupby('country').agg(clicks=('clicks', 'sum'), revenue=('amount', 'sum'))
      df_temp['date'] = str(d_t)
      df_add = pd.concat([df_add, df_temp], ignore_index=False)


    if t_t == 0:
      df_add['cpm'] = df_add['revenue'] / df_add['clicks'] * 1000
      df_add['cpm'] = df_add['cpm'].round(2)
      pop = pd.pivot_table(df_add, index='date', columns='country', values='cpm').T
      pop.fillna(0, inplace=True)
      for df_l in range(len(pop)):
        o_name = str(pop.iloc[df_l].name)
        n_name = str(countries.get(pop.iloc[df_l].name, 'Unknown'))
        pop.rename(index={o_name: n_name}, inplace=True)
      pop = pop.sort_values('country', ascending=True)
      for pic in range(len(pop)):
        plt.subplots(figsize=(5,0.25))
        plt.stairs(pop.iloc[pic], linewidth=5)
        plt.axis('off')
        plt.savefig('pop_' + str(pic) + '.png', dpi=100, bbox_inches='tight')
        plt.close('all')

    elif t_t == 1:
      df_add['cpc'] = df_add['revenue'] / df_add['clicks']
      df_add['cpc'] = df_add['cpc'].round(6)
      push = pd.pivot_table(df_add, index='date', columns='country', values='cpc').T
      push.fillna(0, inplace=True)
      for df_l in range(len(push)):
        o_name = str(push.iloc[df_l].name)
        n_name = str(countries.get(push.iloc[df_l].name, 'Unknown'))
        push.rename(index={o_name: n_name}, inplace=True)
      push = push.sort_values('country', ascending=True)
      for pic in range(len(push)):
        plt.subplots(figsize=(5,0.25))
        plt.stairs(push.iloc[pic], linewidth=5)
        plt.axis('off')
        plt.savefig('push_' + str(pic) + '.png', dpi=100, bbox_inches='tight')
        plt.close('all')

    elif t_t == 2:
      df_add['cpc'] = df_add['revenue'] / df_add['clicks']
      df_add['cpc'] = df_add['cpc'].round(6)
      native = pd.pivot_table(df_add, index='date', columns='country', values='cpc').T
      native.fillna(0, inplace=True)
      for df_l in range(len(native)):
        o_name = str(native.iloc[df_l].name)
        n_name = str(countries.get(native.iloc[df_l].name, 'Unknown'))
        native.rename(index={o_name: n_name}, inplace=True)
      native = native.sort_values('country', ascending=True)
      for pic in range(len(native)):
        plt.subplots(figsize=(5,0.25))
        plt.stairs(native.iloc[pic], linewidth=5)
        plt.axis('off')
        plt.savefig('native_' + str(pic) + '.png', dpi=100, bbox_inches='tight')
        plt.close('all')

    else:
      df_add['cpc'] = df_add['revenue'] / df_add['clicks']
      df_add['cpc'] = df_add['cpc'].round(6)
      inpage = pd.pivot_table(df_add, index='date', columns='country', values='cpc').T
      inpage.fillna(0, inplace=True)
      for df_l in range(len(inpage)):
        o_name = str(inpage.iloc[df_l].name)
        n_name = str(countries.get(inpage.iloc[df_l].name, 'Unknown'))
        inpage.rename(index={o_name: n_name}, inplace=True)
      inpage = inpage.sort_values('country', ascending=True)
      for pic in range(len(inpage)):
        plt.subplots(figsize=(5,0.25))
        plt.stairs(inpage.iloc[pic], linewidth=5)
        plt.axis('off')
        plt.savefig('inpage_' + str(pic) + '.png', dpi=100, bbox_inches='tight')
        plt.close('all')


  with pd.ExcelWriter('cpc_dynamics.xlsx', engine = 'xlsxwriter') as wb:
    pop.reset_index(inplace=True)
    push.reset_index(inplace=True)
    native.reset_index(inplace=True)
    inpage.reset_index(inplace=True)

    pop.to_excel(wb, sheet_name='POP', index=False)
    push.to_excel(wb, sheet_name='PUSH', index=False)
    native.to_excel(wb, sheet_name='NATIVE', index=False)
    inpage.to_excel(wb, sheet_name='INPAGE', index=False)

    for i in range(len(pop.columns)):
      column_length = len(pop.columns[i])
      if i == 0:
        wb.sheets['POP'].set_column(0, 0, column_length + 20)
        wb.sheets['PUSH'].set_column(0, 0, column_length + 20)
        wb.sheets['NATIVE'].set_column(0, 0, column_length + 20)
        wb.sheets['INPAGE'].set_column(0, 0, column_length + 20)
      else:
        wb.sheets['POP'].set_column(i, i, column_length + 1)
        wb.sheets['PUSH'].set_column(i, i, column_length + 1)
        wb.sheets['NATIVE'].set_column(i, i, column_length + 1)
        wb.sheets['INPAGE'].set_column(i, i, column_length + 1)

    sheet_1 = wb.sheets['POP']
    sheet_2 = wb.sheets['PUSH']
    sheet_3 = wb.sheets['NATIVE']
    sheet_4 = wb.sheets['INPAGE']

    list_df_all = []
    list_df_all.append(len(pop))
    list_df_all.append(len(push))
    list_df_all.append(len(native))
    list_df_all.append(len(inpage))

    for i in range(4):
      for j in range(int(list_df_all[i])):
        if i == 0:
          sheet_1.insert_image(j + 1, 8, 'pop_' + str(j) + '.png', {'x_scale': 0.5, 'y_scale': 0.5})
        elif i == 1:
          sheet_2.insert_image(j + 1, 8, 'push_' + str(j) + '.png', {'x_scale': 0.5, 'y_scale': 0.5})
        elif i == 2:
          sheet_3.insert_image(j + 1, 8, 'native_' + str(j) + '.png', {'x_scale': 0.5, 'y_scale': 0.5})
        else:
          sheet_4.insert_image(j + 1, 8, 'inpage_' + str(j) + '.png', {'x_scale': 0.5, 'y_scale': 0.5})

    sheet_1.freeze_panes(1, 1)
    sheet_2.freeze_panes(1, 1)
    sheet_3.freeze_panes(1, 1)
    sheet_4.freeze_panes(1, 1)

  print('STOP BID DYNAMICS ', datetime.now())
  print('--------------------------------')
  time.sleep(5)
  title = (f'Realpush CPM/CPC Dynamics')
  file_uploads_data = [{'file': 'cpc_dynamics.xlsx','title': title}]
  client = slack_sdk.WebClient(token = slack_token)
  client.files_upload_v2(file_uploads = file_uploads_data, channel = slack_channel, initial_comment = 'Realpush CPM/CPC Dynamics')

schedule.every().day.at('08:15').do(job)
while True:
    schedule.run_pending()
    time.sleep(1)
