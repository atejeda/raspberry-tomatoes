from django.shortcuts import render

from django.http import HttpResponse

from lockdown.decorators import lockdown

from google.cloud import bigquery

client = bigquery.Client()

def read_sensor():
    client = bigquery.Client()
    query = 'SELECT * FROM `danarchy-io.stargaze.sensor` ORDER BY date DESC limit 1'
    row = list(client.query(query).result())[0]
    return str(row.date), row.temperature, row.humidity, row.flag_temperature, row.flag_humidity

@lockdown()
def index(request):
    date, temperature, humidity, flagt, flagh = read_sensor()
    return render(request, 'index.html', { 
        'date' : date,
        'temperature' : temperature,
        'humidity' : humidity,
        'flagt' : flagt,
        'flagh' : flagh,
    })
