import requests
import psycopg2
import psycopg2.extras
from datetime import datetime
import logging
from sens import KEY

def fetch_data():

    url = 'http://api.wunderground.com/api/' + KEY + '/conditions/q/CA/Bangalore.json'
    r = requests.get(url).json()
    data = r['current_observation']

    location = data['observation_location']['full'] # city, state and observation location
    weather = data['weather'] # cloud, clear etc
    wind_str = data['wind_string']
    temp = data['temp_f']
    humidity = data['relative_humidity']
    precip = data['precip_today_string']
    icon_url = data['icon_url']
    observation_time = data['observation_time']

    # open db
    try:
        conn = psycopg2.connect(dbname='weather', user='sandeep', host='localhost', password='admin123')
        print "Opened DB Successfully"

    except:
        print datetime.now(), "unablt to connect to database now"
        logging.exception("unable to open database")
        return

    else:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # write data to database
    cur.execute("""INSERT INTO station_reading(location, weather, wind_str, temp, humidity, precip, icon_url, observation_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", (location, weather, wind_str, temp, humidity, precip, icon_url, observation_time))

    conn.commit()
    cur.close()
    conn.close()

    print "data written", datetime.now()


fetch_data()