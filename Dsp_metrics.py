import requests, pandas as pd, matplotlib.pyplot as plt, datetime, slack_sdk, schedule, time, numpy as np, seaborn as sb, matplotlib
from datetime import datetime
from scipy.interpolate import make_interp_spline

matplotlib.use('Agg')

auth_token = ''
slack_token = ''
slack_channel = ''

def job():
    print('START DSP METRICS', datetime.now())
    dsp_id = ['DSP LIST']

    for i in range(13987, 14500):
        dsp_id.append(str(i))

    df_dsp_temp = pd.DataFrame()
    for i in range(len(dsp_id)):
        payload = {'dsp': [dsp_id[i]], 'interval': 6, 'perpage': -1}

        link = 'https://platformapi.net/api/v1/metrics/dsp-response-code'
        response = requests.post(link, headers={'authority': 'platformapi.net',
                                                'authorization': f'{auth_token}',
                                                'path': '/api/v1/metrics/dsp-response-code',
                                                'scheme': 'https',
                                                'referer': 'https://admin.realpush.net/'
                                                },
                                 json=payload
                                 )
        if response.status_code == 200:
            while True:
                try:
                    data = response.json()
                    df = pd.DataFrame(data['data'])
                    df_dsp_temp = pd.concat([df_dsp_temp, df], ignore_index=False)
                    break
                except:
                    print('ошибка сбор ID {dsp_id[i]}')
                    continue
        else:
            continue
    ###########################################################################################################################################################
    list_200 = list('0' * 101)
    dsp_online_count = 0
    indx_time = 0
    for i in range(len(list_200)):
        list_200[i] = int(list_200[i])
    for i in range(len(df_dsp_temp)):
        if "label': 200" not in str(df_dsp_temp.iloc[i]['data']):
            continue
        else:
            dsp_online_count += 1
            raw_data = str(df_dsp_temp.iloc[i]['data'])
            indx_st = raw_data.find("'label': 200")
            raw_data = raw_data[indx_st:]
            indx_en = raw_data.find(']')
            raw_data = raw_data[:indx_en]
            temp_200 = raw_data[23:indx_en]
            code_200 = temp_200.split(',')
            if len(code_200) == 101:
                indx_time = i
            while len(code_200) < 101:
                code_200.append(0)
            for j in range(len(code_200)):
                code_200[j] = int(float(code_200[j]))
            for ji in range(len(list_200)):
                list_200[ji] = list_200[ji] + code_200[ji]

    list_204 = list('0' * 101)
    for i in range(len(list_204)):
        list_204[i] = int(list_204[i])
    for i in range(len(df_dsp_temp)):
        if "label': 204" not in str(df_dsp_temp.iloc[i]['data']):
            continue
        else:
            raw_data = str(df_dsp_temp.iloc[i]['data'])
            indx_st = raw_data.find("'label': 204")
            raw_data = raw_data[indx_st:]
            indx_en = raw_data.find(']')
            raw_data = raw_data[:indx_en]
            temp_204 = raw_data[23:indx_en]
            code_204 = temp_204.split(',')
            while len(code_204) < 101:
                code_204.append(0)
            for j in range(len(code_204)):
                code_204[j] = int(float(code_204[j]))
            for ji in range(len(list_204)):
                list_204[ji] = list_204[ji] + code_204[ji]

    list_799 = list('0' * 101)
    for i in range(len(list_799)):
        list_799[i] = int(list_799[i])
    for i in range(len(df_dsp_temp)):
        if "label': 799" not in str(df_dsp_temp.iloc[i]['data']):
            continue
        else:
            raw_data = str(df_dsp_temp.iloc[i]['data'])
            indx_st = raw_data.find("'label': 799")
            raw_data = raw_data[indx_st:]
            indx_en = raw_data.find(']')
            raw_data = raw_data[:indx_en]
            temp_799 = raw_data[23:indx_en]
            code_799 = temp_799.split(',')
            while len(code_799) < 101:
                code_799.append(0)
            for j in range(len(code_799)):
                code_799[j] = int(float(code_799[j]))
            for ji in range(len(list_799)):
                list_799[ji] = list_799[ji] + code_799[ji]
    # цикл для среднего значения 799
    for i in range(len(list_799)):
        list_799[i] = int(list_799[i] / dsp_online_count)
    ###########################################################################################################################################################
    df = pd.DataFrame({'200': list_200, '204': list_204, '799': list_799})
    ###########################################################################################################################################################
    raw_data = str(df_dsp_temp.iloc[indx_time]['data'])
    indx_st = raw_data.find("{'labels'")
    raw_data = raw_data[indx_st:]
    indx_en = raw_data.find(']')
    raw_data = raw_data[:indx_en]
    temp_time = raw_data[12:indx_en]
    code_time = temp_time.split(',')
    for i in range(len(code_time)):
        code_time[i] = int(code_time[i])
    ###########################################################################################################################################################
    # ИНТЕРПОЛЯЦИЯ
    df = pd.DataFrame({'time': code_time, '200': list_200, '204': list_204, '799': list_799})
    smooth = np.linspace(df.index.min(), df.index.max(), 350)
    df_smooth = pd.DataFrame({data: make_interp_spline(df.index, df[data])(smooth) for data in df.columns})
    # ОБНУЛЕНИЕ ОТРИЦАТЕЛЬНЫХ ЗНАЧЕНИЙ ПОСЛЕ ИНТЕРПОЛЯЦИИ
    for i in range(len(df_smooth)):
        if df_smooth['200'][i] < 0:
            df_smooth['200'][i] = 0
        if df_smooth['204'][i] < 0:
            df_smooth['204'][i] = 0
        if df_smooth['799'][i] < 0:
            df_smooth['799'][i] = 0
    # КОНВЕРТАЦИЯ TIMESTAMP
    df_smooth['time'] = df_smooth['time'].astype(int)
    for i in range(len(df_smooth)):
        df_smooth['time'][i] = datetime.fromtimestamp(df_smooth['time'][i])
    ###########################################################################################################################################################
    plt.style.use('https://github.com/dhaitz/matplotlib-stylesheets/raw/master/pitayasmoothie-dark.mplstyle')
    x = df_smooth['time']
    ay = df_smooth['200']
    by = df_smooth['204']
    cy = df_smooth['799']
    y = np.vstack([ay, cy, by])
    fig, ax = plt.subplots(figsize=(20, 8))
    avg_200 = round((df['200'].mean()), 2)
    avg_204 = round((df['204'].mean()), 2)
    avg_799 = round((df['799'].mean()), 2)
    cols = ['#3eff00', '#ff0002', '#ffb600']
    ax.stackplot(x, y, labels=[f'200 [{avg_200}]', f'799 [{avg_799}]', f'204 [{avg_204}]'], colors=cols)
    ax.legend(loc='upper right')
    plt.title('DSP Main Metrics 7 Days')
    plt.ylabel('Status Code per second')
    plt.xticks(rotation=90)

    xx_200 = np.arange(df_smooth['200'].size)
    fit_200 = np.polyfit(xx_200, df_smooth['200'], deg=1)
    fit_function_200 = np.poly1d(fit_200)

    xx_204 = np.arange(df_smooth['204'].size)
    fit_204 = np.polyfit(xx_204, df_smooth['204'], deg=1)
    fit_function_204 = np.poly1d(fit_204)

    xx_799 = np.arange(df_smooth['799'].size)
    fit_799 = np.polyfit(xx_799, df_smooth['799'], deg=1)
    fit_function_799 = np.poly1d(fit_799)

    sb.lineplot(x=df_smooth['time'].values, y=fit_function_200(xx_200), color='white', linestyle='dotted')
    sb.lineplot(x=df_smooth['time'].values, y=fit_function_204(xx_204), color='white', linestyle='dotted')
    sb.lineplot(x=df_smooth['time'].values, y=fit_function_799(xx_799), color='white', linestyle='dotted')

    pic = fig.savefig('dsp_metrics.png', dpi=100, bbox_inches='tight')
    plt.close('all')
    print('STOP DSP METRICS', datetime.now())
    print('--------------------------------')

    time.sleep(5)
    file_uploads_data = [{'file': 'dsp_metrics.png', 'title': 'DSP Main Metrics 7 Days'}]
    client = slack_sdk.WebClient(token=slack_token)
    client.files_upload_v2(file_uploads=file_uploads_data, channel=slack_channel,
                           initial_comment='DSP Main Metrics 7 Days')


schedule.every().day.at('10:00').do(job)
while True:
    schedule.run_pending()
    time.sleep(1)
