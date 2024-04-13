**Trend.py**

*Генерация по-часового тренда выручки в сравнении с предыдущим днем. Публикация изображения осуществляется в SLACK*

![trend_on_yestarday](https://github.com/TesLineX/Programmatic/assets/56605777/b30c9822-9b6b-45b1-b733-c4359109d480)

![trend_on_yestarday (1)](https://github.com/TesLineX/Programmatic/assets/56605777/ca1e6cd4-0d45-4a30-b623-e4239ce0d3a6)

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
