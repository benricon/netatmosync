from datetime import datetime, timedelta
import requests
import time
from socket import socket
from time import sleep


class netatmosync():
    # Username for netatmo
    username = ""
    # Password for netatmon
    password = ""
    # Client id from netatmo dev
    client_id = ""
    # Client secrent from netatmo dev
    client_secret = ""
    # ID of weather station
    master_id = ""
    # url or ip for carbon server
    carbon_server = ""
    # port number for plain text carbon interface.
    carbon_port = 2003
    # Amount(minutes) of data to be transferred when in sync mode
    sync_delta = 15
    # Basepath for carbon metric remeber to put a . in the end
    base_metric_path = ""
    # Set to true if old data should be backfilled before entering sync mode
    backfill_data = True
    # if backfill data is set true data will be fetched from this date
    last_datafetch = datetime(year=2015, month=1, day=1)

    def authenticate(self):
        payload = {
            'grant_type': 'password',
            'username': self.username,
            'password': self.password,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': 'read_station'
            }
        try:
            response = requests.post("https://api.netatmo.com/oauth2/token",
                                     data=payload)
            response.raise_for_status()
            self.access_token = response.json()["access_token"]
            self.refresh_token = response.json()["refresh_token"]
            self.expires_in = response.json()["expires_in"] + time.time()
        except requests.exceptions.HTTPError as error:
            print(error.response.status_code, error.response.text)

    def refresh_auth(self):
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            }
        try:
            response = requests.post("https://api.netatmo.com/oauth2/token",
                                     data=payload)
            response.raise_for_status()
            self.access_token = response.json()["access_token"]
            self.refresh_token = response.json()["refresh_token"]
            self.expires_in = response.json()["expires_in"] + time.time()
        except requests.exceptions.HTTPError as error:
            print(error.response.status_code, error.response.text)

    def get_modules(self):
        params = {
            'access_token': self.access_token,
            'device_id': self.master_id
            }

        try:
            request_url = "https://api.netatmo.com/api/getstationsdata"
            response = requests.post(request_url,
                                     params=params)
            response.raise_for_status()
            data = response.json()["body"]['devices'][0]
            self.master_id = data['_id']
            self.modules = {}
            self.modules[data['_id']] = {
                'data_type': data['data_type'],
                'module_name': data['module_name']
                }
            for module in data['modules']:
                # the below condition is compensating for and error in the api
                if module['data_type'] == ['Wind']:
                    self.modules[module['_id']] = {
                        'data_type': ['WindStrength',
                                      'WindAngle',
                                      'Guststrength',
                                      'GustAngle'],
                        'module_name': module['module_name']
                        }
                else:
                    self.modules[module['_id']] = {
                        'data_type': module['data_type'],
                        'module_name': module['module_name']
                        }

        except requests.exceptions.HTTPError as error:
            print(error.response.status_code, error.response.text)

    def get_data(self, host_id, m_id, date_begin, date_end, data_type):
        payload = {
            'access_token': self.access_token,
            'device_id': host_id,
            'module_id': m_id,
            'scale': 'max',
            'type': data_type,
            'date_begin': int(datetime.timestamp(date_begin)),
            'date_end': int(datetime.timestamp(date_end)),
            'optimize': 'false'
            }
        try:
            request_url = "https://api.netatmo.com/api/getmeasure"
            response = requests.post(request_url,
                                     data=payload)
            response.raise_for_status()
            return response.json()['body']

        except requests.exceptions.HTTPError as error:
            print(error.response.status_code, error.response.text)
            return error.response.status_code

    def sync_data(self, d_hours=24, d_minutes=0):
        sock = socket()
        try:
            sock.connect((self.carbon_server, self.carbon_port))
        except:
            raise Exception('Could not connect to carbon-server')

        te_time = self.last_datafetch + timedelta(hours=d_hours,
                                                  minutes=d_minutes)
        for m_id, module in self.modules.items():
            print('----------------')
            print(module['data_type'])
            output = self.get_data(self.master_id,
                                   m_id,
                                   self.last_datafetch,
                                   te_time,
                                   ','.join(module['data_type']))
            if output != 400 and output != []:
                for timestamp, data in output.items():
                    for i in range(len(module['data_type'])):
                        message = (self.base_metric_path +
                                   module['module_name'] +
                                   '.' +
                                   module['data_type'][i] +
                                   ' ' + str(data[i]) +
                                   ' ' +
                                   timestamp +
                                   '\n')
                        sock.sendall(message.encode())
        sock.close()
        self.last_datafetch = te_time

    def check_auth(self):
        if self.expires_in - 30 <= time.time():
            self.refresh_auth()
            print("Auth refreshed")
        else:
            print("Auth refresh not needed")

    def __init__(self):
        self.authenticate()
        self.get_modules()
        if not self.backfill_data:
            self.last_datafetch = datetime.now()


n = netatmosync()
while True:
    while (
        n.last_datafetch + timedelta(hours=24) < datetime.now() and
        n.backfill_data is True
    ):
        try:
            print("Backfill sync",
                  datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))
            n.check_auth()
            n.sync_data()
            sleep(30)
        except Exception as e:
            print(e)
            sleep(10)

    while n.last_datafetch + timedelta(hours=24) > datetime.now():
        print("Online sync",
              datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))
        n.check_auth()
        next_read_time = n.last_datafetch + timedelta(minutes=n.sync_delta + 2)
        if next_read_time < datetime.now():
            while True:
                try:
                    print("Online sync",
                          datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))
                    n.sync_data(
                        d_hours=0,
                        d_minutes=n.sync_delta
                        )
                    break
                except Exception as e:
                    print(e)
                    sleep(10)
                    continue
        sleep(60)
