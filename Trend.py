import requests as rq, pandas as pd, time, pytz, json, slack_sdk, schedule, matplotlib.pyplot as plt, matplotlib
from datetime import timedelta, datetime

pd.options.mode.chained_assignment = None
matplotlib.use('Agg')
token = ''
slack_token = ''
slack_channel = ''


def job():
    time_format_trend = '%Y-%m-%d'
    now_now = datetime.now(pytz.utc)
    hour_trend = int(now_now.hour)

    now_trend = datetime.now(pytz.utc)
    past_trend = now_trend - timedelta(days = 1)
    now_trend = now_trend.strftime(time_format_trend)
    past_trend = past_trend.strftime(time_format_trend)

    if hour_trend == 0:
        now_trend = datetime.now(pytz.utc) - timedelta(days = 1)
        past_trend = now_trend - timedelta(days = 1)
        now_trend = now_trend.strftime(time_format_trend)
        past_trend = past_trend.strftime(time_format_trend)

    link_1_trend = f'https://backapi.admy.com/api/v1/reports/dsp/byhours/{now_trend}?token={token}&format=json'
    link_2_trend = f'https://backapi.admy.com/api/v1/reports/dsp/byhours/{past_trend}?token={token}&format=json'
    response_1_trend = rq.get(link_1_trend)
    response_2_trend = rq.get(link_2_trend)
    data_1_trend = response_1_trend.json()
    data_2_trend = response_2_trend.json()
    df_1_trend = pd.DataFrame(data_1_trend['data'])
    df_2_trend = pd.DataFrame(data_2_trend['data'])
    df_1_trend = df_1_trend.groupby('hour').agg(Clicks=('clicks', 'sum'), amount=('amount', 'sum'))
    df_2_trend = df_2_trend.groupby('hour').agg(Clicks=('clicks', 'sum'), amount=('amount', 'sum'))
    if hour_trend != 0:
        df_1_trend['Clicks'].iloc[-1] = 0
        df_1_trend['amount'].iloc[-1] = 0

    ##############################################################################
    df_1_trend = df_1_trend.reset_index()
    df_2_trend = df_2_trend.reset_index()

    count_trend = 0
    if len(df_1_trend) < 24:
        count_trend = 24 - len(df_1_trend)

    if hour_trend != 0:
        for i_trend in range(count_trend):
            df_1_trend.loc[len(df_1_trend.index)] = [len(df_1_trend.index),
                                                     0,
                                                     0
                                                     ]
    ##############################################################################
    # счетчик для тренда, сколько строк не нулевых и сумма для этого периода
    trend = 0
    for i_trend in range(len(df_1_trend)):
        if df_1_trend['amount'].iloc[i_trend] > 0:
            trend += 1

    sum_past_trend = df_2_trend['amount'].iloc[:trend].sum()
    sum_now_trend = df_1_trend['amount'].iloc[:trend].sum()

    procent = ((1 / sum_past_trend * sum_now_trend) * 100).round(1)
    ##############################################################################
    fig_trend, ax_trend = plt.subplots(figsize=(12, 5))
    if sum_now_trend > sum_past_trend:
        plt.fill_between(df_1_trend['hour'], df_1_trend['amount'], color='g', alpha=0.70)
    else:
        plt.fill_between(df_1_trend['hour'], df_1_trend['amount'], color='r', alpha=0.5)
    plt.fill_between(df_2_trend['hour'], df_2_trend['amount'], color='grey', alpha=0.55)
    plt.xticks(df_1_trend['hour'])
    plt.xlabel('Hour', size=12)
    plt.ylabel('Revenue, $', size=12)
    plt.legend([now_trend, past_trend], loc='upper right')

    if sum_now_trend > sum_past_trend:
        ax_trend.set_title(f'+{(procent - 100).round(1)}% (+${(sum_now_trend - sum_past_trend).round(2)})', fontsize=15,
                           color='g', fontweight='bold')
    else:
        ax_trend.set_title(f'{(procent - 100).round(1)}% (-${(sum_past_trend - sum_now_trend).round(2)})', fontsize=15,
                           color='r', fontweight='bold')

    plt.savefig('trend_on_yestarday.png', dpi=100)
    plt.close('all')
    ##############################################################################
    link_messages_trend = 'https://slack.com/api/conversations.history'
    data_trend = {'channel': slack_channel}
    resp_mess_trend = rq.post(link_messages_trend, headers={
        'authorization': f'Bearer {slack_token}'}, json=data_trend)
    row_messages_trend = resp_mess_trend.json()
    messages_trend = pd.DataFrame(row_messages_trend['messages'])
    messages_trend = messages_trend[['text', 'files', 'ts']]
    messages_trend['timestamp'] = ''
    messages_trend['date'] = ''
    for i_trend in range(len(messages_trend)):
        temp_trend = str(messages_trend['files'].iloc[i_trend])
        indx_st_trend = temp_trend.find("'timestamp':")
        temp_trend = temp_trend[indx_st_trend:]
        indx_en_trend = temp_trend.find(',')
        temp_trend = temp_trend[13:indx_en_trend]
        messages_trend['timestamp'].iloc[i_trend] = temp_trend
        if messages_trend['timestamp'].iloc[i_trend] == '':
            continue
        else:
            ts_trend = int(messages_trend['timestamp'].iloc[i_trend])
            messages_trend['date'].iloc[i_trend] = datetime.utcfromtimestamp(ts_trend).strftime('%Y-%m-%d')

    messages_trend.drop('files', axis=1, inplace=True)
    messages_trend['date'] = pd.to_datetime(messages_trend['date'])
    ##############################################################################
    indx_date_trend = None
    if messages_trend['text'].str.contains('Trend').any() == True:
        for i_trend in range(len(messages_trend)):
            if messages_trend['text'].iloc[i_trend] == 'Trend':
                indx_date_trend = i_trend
                break
        if pd.to_datetime(now_trend, format='%Y%m%d %H:%M:%S') == messages_trend['date'].iloc[indx_date_trend]:
            link_messages_trend = 'https://slack.com/api/chat.delete'
            ts_del_trend = messages_trend['ts'].iloc[indx_date_trend]
            data_trend = {'channel': slack_channel, 'ts': ts_del_trend}
            resp_mess_trend = rq.post(link_messages_trend, headers={
                'authorization': f'Bearer {slack_token}'}, json=data_trend)
            file_uploads_data = [{'file': 'trend_on_yestarday.png', 'title': 'Trend'}]
            client = slack_sdk.WebClient(token=slack_token)
            client.files_upload_v2(file_uploads=file_uploads_data, channel=slack_channel, initial_comment='Trend')
        else:
            file_uploads_data = [{'file': 'trend_on_yestarday.png', 'title': 'Trend'}]
            client = slack_sdk.WebClient(token=slack_token)
            client.files_upload_v2(file_uploads=file_uploads_data, channel=slack_channel, initial_comment='Trend')
    else:
        file_uploads_data = [{'file': 'trend_on_yestarday.png', 'title': 'Trend'}]
        client = slack_sdk.WebClient(token=slack_token)
        client.files_upload_v2(file_uploads=file_uploads_data, channel=slack_channel, initial_comment='Trend')


schedule.every().hour.at(':10').do(job)
while True:
    schedule.run_pending()
    time.sleep(1)
