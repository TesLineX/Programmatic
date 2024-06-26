**Trend.py**

*Генерация изображения по-часового тренда дохода. Публикация осуществляется в SLACK*

![trend_on_yestarday](https://github.com/TesLineX/Programmatic/assets/56605777/571fb574-675e-47a9-8848-736089ec7578)
![trend_on_yestarday (1)](https://github.com/TesLineX/Programmatic/assets/56605777/3b38906c-a8af-4239-9033-322ef2afc77f)


Необходимые для заполнения переменные:

```python
token = ''
slack_token = ''
slack_channel = ''
```

Периодичность выгрузки указывается здесь:

```python
schedule.every().hour.at(':10').do(job)
```



**Match.py**

*Сопоставление общей статистики с DSP партнером для анализа расхождения у Feed/Endpoint и его профита.*
*Выводит таблицу в Colab или Jupiter.*


|Date|dsp\_id|DSP Clicks|RealPush Clicks|DSP Revenue|RealPush Revenue|Publisher Revenue|PROFIT|SHARE|CLK DSC %|REV DSC %|
|---|---|---|---|---|---|---|---|---|---|---|
|2024-04-01|12482|993 109|1 055 947|234\.71|248\.78|85\.88|148\.83|37 \<\> 63|6\.0|5\.7|
|2024-04-02|12482|915 052|952 906|220\.15|228\.73|84\.51|135\.64|38 \<\> 62|4\.0|3\.8|
|2024-04-03|12482|896 385|977 628|186\.77|202\.52|78\.42|108\.35|42 \<\> 58|8\.3|7\.8|
|2024-04-04|12482|818 422|953 447|159\.6|184\.65|73\.29|86\.31|46 \<\> 54|14\.2|13\.6|
|2024-04-05|12482|792 094|949 237|153\.88|182\.05|72\.11|81\.77|47 \<\> 53|16\.6|15\.5|
|2024-04-06|12482|560 089|668 596|101\.05|119\.05|46\.98|54\.07|46 \<\> 54|16\.2|15\.1|
|2024-04-07|12482|797 767|920 666|119\.97|137\.33|54\.05|65\.92|45 \<\> 55|13\.3|12\.6|
|2024-04-08|12482|632 166|717 372|108\.39|122\.3|43\.34|65\.05|40 \<\> 60|11\.9|11\.4|
|2024-04-09|12482|559 093|628 977|107\.87|121\.36|40\.12|67\.75|37 \<\> 63|11\.1|11\.1|
|2024-04-10|12482|821 379|926 418|149\.75|168\.86|54\.21|95\.54|36 \<\> 64|11\.3|11\.3|
|2024-04-11|12482|1 179 761|1 329 371|222\.23|250\.07|80\.1|142\.13|36 \<\> 64|11\.3|11\.1|
|2024-04-12|12482|1 180 004|1 303 267|215\.52|237\.9|78\.52|137\.0|36 \<\> 64|9\.5|9\.4|
|2024-04-13|12482|527 351|571 464|91\.39|99\.22|33\.31|58\.08|36 \<\> 64|7\.7|7\.9|
|Total|---|10 672 672|11 955 296|2071\.2799999999997|2302\.8199999999997|824\.8400000000001|1246\.44|40 \<\> 60|10\.7|10\.1|

**Pie.py**

Круговая диаграмма, представляющая собой долю оборота по каждому партнеру.
Рекламные кампании(dsp) агрегируются по общему названию, в итоге известна абсолютная величина.
Реализовано из-за отсутствия агрегации в дашборде платформы AdMy. Публикация изображения происходит в Slack.
Запускается вручную, из-за нестабильного соединения API и облачного сервера, и других ошибок (http[502, 503]).

![dsp_20_pie](https://github.com/TesLineX/Programmatic/assets/56605777/aa8cc5cf-7119-44b5-92f7-da1f0882bcd8)


**DSP_metrics.py**

*Накопительная диаграмма с основными показателями производительности сети*
+ 200 - ответы с креативом
+ 204 - пустые ответы, нет деманда
+ 799 - троттлинг системы

*На момент создания, у платформы AdMy отсутствовала возможность просмотра подобной статистики за более чем 12 часов по всем сущностям. Данный скрипт позволяет собрать данные за последние 7 дней по всем сущностям системы.*

![dsp_metrics](https://github.com/TesLineX/Programmatic/assets/56605777/5242a8dc-d476-4f24-8d7f-0ad6d35d65b6)


**Dsp_metrics.py**

Динамика изменений средней цены по каждому региону. Генерируется Excel файл, автоматически выгружается в Slack.
Характер графика меняется в следующем блоке:

```python
     for pic in range(len(inpage)):
        plt.subplots(figsize=(5,0.25))
        >>>plt.stairs(inpage.iloc[pic], linewidth=5)<<<
        plt.axis('off')
        plt.savefig('inpage_' + str(pic) + '.png', dpi=100, bbox_inches='tight')
        plt.close('all')
```

![image](https://github.com/TesLineX/Programmatic/assets/56605777/8bd08d6b-4293-4388-be95-d7d2d7cbc4f0)


**Revenue_dynamics.py**

Столбчатая диаграмма под каждый отдельный рекламный формат.

![revenue_dynamics](https://github.com/TesLineX/Programmatic/assets/56605777/d1bc35cf-06a9-4d13-b21b-44be37ca3c03)

