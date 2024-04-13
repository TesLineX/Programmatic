import requests as rq, pandas as pd, matplotlib.pyplot as plt, seaborn as sb, schedule, time, slack_sdk, matplotlib, re
from datetime import datetime
from datetime import timedelta
import matplotlib.gridspec as gspec
import warnings

warnings.filterwarnings("ignore")
matplotlib.use('Agg')
pd.options.mode.chained_assignment = None

token = ''
slack_token = ''
slack_channel = ''

def job():
    pattern = "-(\d{2}-\d{2})"
    print('START REVENUE DYNAMICS ', datetime.now())

    now = datetime.now()
    date_target = now - timedelta(days=30)
    time_format = '%Y-%m-%d'

    df_push_c = pd.DataFrame()
    for i in range(0, 30):
        traffic_type = '2'
        d_t = date_target + timedelta(days=i)
        d_t = d_t.strftime(time_format)

        def link_push(d_t):
            text = f'https://backapi.admy.com/api/v1/reports/dsp/bycountries?token={token}&date_from={d_t}&date_to={d_t}&traffic_type={traffic_type}'
            return text

        while True:
            try:
                time.sleep(1)  # добавлена пауза из-за нагрузки на сервер платформы
                url = link_push(d_t)
                response = rq.get(url)
                data = response.json()
                df_push = pd.DataFrame(data['data'])
                temp = re.search(pattern, str(d_t))
                df_push['Date'] = temp[1]
                df_push = df_push.groupby('Date').agg(clicks=('clicks', 'sum'), revenue=('amount', 'sum'))
                df_push['revenue'] = df_push['revenue'].round(4)
                df_push_c = pd.concat([df_push_c, df_push], ignore_index=False)
                # print('push:', d_t)
                break
            except:
                print(f'ОШИБКА PUSH {url}')
                print(response.status_code)
                continue
    ####################################################################################################################################################################################
    df_native_c = pd.DataFrame()
    for i in range(0, 30):
        traffic_type = '3'
        d_t = date_target + timedelta(days=i)
        d_t = d_t.strftime(time_format)

        def link_native(d_t):
            text = f'https://backapi.admy.com/api/v1/reports/dsp/bycountries?token={token}&date_from={d_t}&date_to={d_t}&traffic_type={traffic_type}'
            return text

        while True:
            try:
                time.sleep(1)  # добавлена пауза из-за нагрузки на сервер платформы
                url = link_native(d_t)
                response = rq.get(url)
                data = response.json()
                df_native = pd.DataFrame(data['data'])
                temp = re.search(pattern, str(d_t))
                df_native['Date'] = temp[1]
                df_native = df_native.groupby('Date').agg(clicks=('clicks', 'sum'), revenue=('amount', 'sum'))
                df_native['revenue'] = df_native['revenue'].round(4)
                df_native_c = pd.concat([df_native_c, df_native], ignore_index=False)
                # print('native:', d_t)
                break
            except:
                print(f'ОШИБКА NATIVE {url}')
                print(response.status_code)
                continue
                ####################################################################################################################################################################################
    df_inpage_c = pd.DataFrame()
    for i in range(0, 30):
        traffic_type = '8'
        d_t = date_target + timedelta(days=i)
        d_t = d_t.strftime(time_format)

        def link_inpage(d_t):
            text = f'https://backapi.admy.com/api/v1/reports/dsp/bycountries?token={token}&date_from={d_t}&date_to={d_t}&traffic_type={traffic_type}'
            return text

        while True:
            try:
                time.sleep(1)  # добавлена пауза из-за нагрузки на сервер платформы
                url = link_inpage(d_t)
                response = rq.get(url)
                data = response.json()
                df_inpage = pd.DataFrame(data['data'])
                temp = re.search(pattern, str(d_t))
                df_inpage['Date'] = temp[1]
                df_inpage = df_inpage.groupby('Date').agg(clicks=('clicks', 'sum'), revenue=('amount', 'sum'))
                df_inpage['revenue'] = df_inpage['revenue'].round(4)
                df_inpage_c = pd.concat([df_inpage_c, df_inpage], ignore_index=False)
                # print('inpage:', d_t)
                break
            except:
                print(f'ОШИБКА INPAGE {url}')
                print(response.status_code)
                continue
    ####################################################################################################################################################################################
    df_pop_c = pd.DataFrame()
    for i in range(0, 30):
        traffic_type = '0'
        d_t = date_target + timedelta(days=i)
        d_t = d_t.strftime(time_format)

        def link_pop(d_t):
            text = f'https://backapi.admy.com/api/v1/reports/dsp/bycountries?token={token}&date_from={d_t}&date_to={d_t}&traffic_type={traffic_type}'
            return text

        while True:
            try:
                time.sleep(1)  # добавлена пауза из-за нагрузки на сервер платформы
                url = link_pop(d_t)
                response = rq.get(url)
                data = response.json()
                df_pop = pd.DataFrame(data['data'])
                temp = re.search(pattern, str(d_t))
                df_pop['Date'] = temp[1]
                df_pop = df_pop.groupby('Date').agg(clicks=('clicks', 'sum'), revenue=('amount', 'sum'))
                df_pop['revenue'] = df_pop['revenue'].round(4)
                df_pop_c = pd.concat([df_pop_c, df_pop], ignore_index=False)
                # print('pop:', d_t)
                break
            except:
                print(f'ОШИБКА POP {url}')
                print(response.status_code)
                continue
    ####################################################################################################################################################################################
    df_banner_c = pd.DataFrame()
    for i in range(0, 30):
        traffic_type = '4'
        d_t = date_target + timedelta(days=i)
        d_t = d_t.strftime(time_format)

        def link_banner(d_t):
            text = f'https://backapi.admy.com/api/v1/reports/dsp/bycountries?token={token}&date_from={d_t}&date_to={d_t}&traffic_type={traffic_type}'
            return text

        while True:
            try:
                time.sleep(1)  # добавлена пауза из-за нагрузки на сервер платформы
                url = link_banner(d_t)
                response = rq.get(url)
                data = response.json()
                df_banner = pd.DataFrame(data['data'])
                temp = re.search(pattern, str(d_t))
                df_banner['Date'] = temp[1]
                df_banner = df_banner.groupby('Date').agg(impressions=('imps', 'sum'), revenue=('amount', 'sum'))
                df_banner['revenue'] = df_banner['revenue'].round(4)
                df_banner_c = pd.concat([df_banner_c, df_banner], ignore_index=False)
                # print('banner:', d_t)
                break
            except:
                print(f'ОШИБКА BANNER {url}')
                print(response.status_code)
                continue
    ####################################################################################################################################################################################

    a, b, c, d, e = df_push_c.reset_index(), df_native_c.reset_index(), df_inpage_c.reset_index(), df_pop_c.reset_index(), df_banner_c.reset_index()
    a['revenue'] = a['revenue'].astype(int)
    b['revenue'] = b['revenue'].astype(int)
    c['revenue'] = c['revenue'].astype(int)
    d['revenue'] = d['revenue'].astype(int)
    e['revenue'] = e['revenue'].astype(int)

    fig = plt.figure(figsize=(24, 18))
    gs = gspec.GridSpec(3, 2)

    axs1 = plt.subplot(gs[0, 0])
    axs2 = plt.subplot(gs[0, 1])
    axs3 = plt.subplot(gs[1, 0])
    axs4 = plt.subplot(gs[1, 1])
    axs5 = plt.subplot(gs[2, 0])

    sb.set_style('dark')

    axs1.bar(a['Date'], a['revenue'], width=0.5, edgecolor='black', align='center')
    axs2.bar(b['Date'], b['revenue'], width=0.5, edgecolor='black', align='center')
    axs3.bar(c['Date'], c['revenue'], width=0.5, edgecolor='black', align='center')
    axs4.bar(d['Date'], d['revenue'], width=0.5, edgecolor='black', align='center')
    axs5.bar(e['Date'], e['revenue'], width=0.5, edgecolor='black', align='center')

    axs1.set(ylim=[0, a['revenue'].max() * 1.05])
    axs2.set(ylim=[0, b['revenue'].max() * 1.05])
    axs3.set(ylim=[0, c['revenue'].max() * 1.05])
    axs4.set(ylim=[0, d['revenue'].max() * 1.05])
    axs5.set(ylim=[0, e['revenue'].max() * 1.05])

    gs.update(hspace=0.25)

    axs1.set_xlabel('Date', fontweight='bold', fontsize=12)
    axs1.set_ylabel('Revenue,$', fontweight='bold', fontsize=12)
    axs1.set_title('PUSH', fontweight='bold', fontsize=16)
    axs2.set_xlabel('Date', fontweight='bold', fontsize=12)
    axs2.set_ylabel('Revenue,$', fontweight='bold', fontsize=12)
    axs2.set_title('NATIVE', fontweight='bold', fontsize=16)
    axs3.set_xlabel('Date', fontweight='bold', fontsize=12)
    axs3.set_ylabel('Revenue,$', fontweight='bold', fontsize=12)
    axs3.set_title('INPAGE', fontweight='bold', fontsize=16)
    axs4.set_xlabel('Date', fontweight='bold', fontsize=12)
    axs4.set_ylabel('Revenue,$', fontweight='bold', fontsize=12)
    axs4.set_title('POPUNDER', fontweight='bold', fontsize=16)
    axs5.set_xlabel('Date', fontweight='bold', fontsize=12)
    axs5.set_ylabel('Revenue,$', fontweight='bold', fontsize=12)
    axs5.set_title('BANNER', fontweight='bold', fontsize=16)

    value_push, value_native, value_inpage, value_pop, value_banner = [], [], [], [], []

    for i, j in enumerate(a['revenue']):
        value_push.append(j)
    for index in range(len(value_push)):
        axs1.text(index - 0.5, value_push[index], value_push[index], fontsize=7, color='black', fontweight='bold')

    for i, j in enumerate(b['revenue']):
        value_native.append(j)
    for index in range(len(value_native)):
        axs2.text(index - 0.5, value_native[index], value_native[index], fontsize=7, color='black', fontweight='bold')

    for i, j in enumerate(c['revenue']):
        value_inpage.append(j)
    for index in range(len(value_inpage)):
        axs3.text(index - 0.5, value_inpage[index], value_inpage[index], fontsize=7, color='black', fontweight='bold')

    for i, j in enumerate(d['revenue']):
        value_pop.append(j)
    for index in range(len(value_pop)):
        axs4.text(index - 0.5, value_pop[index], value_pop[index], fontsize=7, color='black', fontweight='bold')

    for i, j in enumerate(e['revenue']):
        value_banner.append(j)
    for index in range(len(value_banner)):
        axs5.text(index - 0.5, value_banner[index], value_banner[index], fontsize=7, color='black', fontweight='bold')

    for ax in fig.axes:
        matplotlib.pyplot.sca(ax)
        plt.xticks(rotation=90)

    pic = fig.savefig('revenue_dynamics.png', dpi=100, bbox_inches='tight')
    plt.close('all')
    print('STOP REVENUE DYNAMICS ', datetime.now())
    print('--------------------------------')

    time.sleep(5)
    file_uploads_data = [{'file': 'revenue_dynamics.png', 'title': 'Revenue Dynamics by Traffic Type'}]
    client = slack_sdk.WebClient(token=slack_token)
    client.files_upload_v2(file_uploads=file_uploads_data, channel=slack_channel,
                           initial_comment='Revenue Dynamics by Traffic Type')


schedule.every().day.at('06:00').do(job)
while True:
    schedule.run_pending()
    time.sleep(30)
