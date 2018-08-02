import time
import json
import requests
import datetime

#datetime.datetime(2018,7,31,15,0).timestamp()
currdate = datetime.datetime.now()
print(currdate)
startdate = currdate + datetime.timedelta(0,0,0,0,-5)
print(startdate)
starttime = int(startdate.strftime("%s"))

url = 'https://blinkhealth.zendesk.com/api/v2/channels/voice/incremental/calls.json?start_time=' + str(starttime) + '&include=users'
user = 'support@blinkhealth.com' + '/token'
pwd = ''

while(1):
    # Do the HTTP get request
    response = requests.get(url, auth=(user, pwd))
    # Check for HTTP codes other than 200
    if response.status_code != 200:
        print('Status:', response.status_code, 'Problem with the request. Exiting.')
    # Decode the JSON response into a dictionary and use the data
    data = response.json()
    type(data)
    jsondata = json.dumps(data)
    url = data["next_page"]
    #print( 'First group = ', jsondata)
    print( 'Next page  url = ', url)
    with open('./data/zencalls.json','w') as zenfile:
        zenfile.write(jsondata)
    print( 'Sleeping ...')
    time.sleep(60)
