#This code sample uses requests (HTTP library)
import datetime
import requests
import time
from socket import socket
from time import sleep



class netatmosync():
    username = ""
    password = ""
    client_id = ""
    client_secret = ""
    master_id = ''
    carbon_server = "" 
    carbon_port = 0000
    last_datafetch = datetime.datetime(year=2015, month=1, day=1)
    sync_delta = 15
    base_metric_path = ''
    
    def authenticate(self):
        
        
        payload = {
            'grant_type': 'password',
            'username': self.username,
            'password': self.password,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': 'read_station read_homecoach read_thermostat read_presence'
            }
        try:
            response = requests.post("https://api.netatmo.com/oauth2/token", data=payload)
            response.raise_for_status()
            self.access_token=response.json()["access_token"]
            self.refresh_token=response.json()["refresh_token"]
            self.expires_in = response.json()["expires_in"] + time.time()
        except requests.exceptions.HTTPError as error:
            print(error.response.status_code, error.response.text)





    def refresh_auth(self):
        payload = {
            'grant_type': 'refresh_token',
            'username': self.username,
            'password': self.password,
            'client_id':self.client_id,
            'client_secret': self.client_secret,
            }     
        try:
            response = requests.post("https://api.netatmo.com/oauth2/token", data=payload)
            response.raise_for_status()
            self.access_token=response.json()["access_token"]
            self.refresh_token=response.json()["refresh_token"]
            self.expires_in = response.json()["expires_in"] + time.time()
        except requests.exceptions.HTTPError as error:
            print(error.response.status_code, error.response.text)
            
    def get_modules(self):
        params = {
            'access_token': self.access_token,
            'device_id': self.master_id
            }
        
        try:
            response = requests.post("https://api.netatmo.com/api/getstationsdata", params=params)
            response.raise_for_status()

            data = response.json()["body"]['devices'][0]
            self.master_id = data['_id']
            self.modules = {}
            self.modules[data['_id']] = {
                'data_type' : data['data_type'],
                'module_name' : data['module_name']
                }
            for module in data['modules']:
                if module['data_type'] == ['Wind']:
                    self.modules[module['_id']] = {
                        'data_type' : ['WindStrength','WindAngle', 'Guststrength', 'GustAngle'], 
                        'module_name' : module['module_name']
                        }
                else:
                    self.modules[module['_id']] = {
                        'data_type' : module['data_type'], 
                        'module_name' : module['module_name']
                        }

                    
            
            print(self.modules)
        except requests.exceptions.HTTPError as error:
            print(error.response.status_code, error.response.text)

    def get_data(self, id, m_id, date_begin, date_end, data_type):
        payload = {
            'access_token': self.access_token,
            'device_id': id,
            'module_id': m_id,
            'scale':'max',
            'type': data_type,
            'date_begin' : int(datetime.datetime.timestamp(date_begin)),
            'date_end' : int(datetime.datetime.timestamp(date_end)),
            'optimize' : 'false'
            }     
        print(payload)
        try:
            response = requests.post("https://api.netatmo.com/api/getmeasure", data=payload)
            response.raise_for_status()
            return response.json()['body']
            
        except requests.exceptions.HTTPError as error:
            print(error.response.status_code, error.response.text)
            return error.response.status_code
            
   


    def sync_data(self, delta_hours = 24, delta_minutes = 0):
        sock = socket()
        try:
            sock.connect((self.carbon_server, self.carbon_port))
        except:
            raise Exception('Could not connect to carbon-server')
        
        te_time = self.last_datafetch + datetime.timedelta(hours = delta_hours, minutes = delta_minutes)
        for m_id, module in self.modules.items():
            print('----------------')
            print(module['data_type'])
            output = self.get_data(self.master_id,m_id, self.last_datafetch, te_time , ','.join(module['data_type']))
            if output != 400 and output != []:
                for timestamp, data in output.items():
                    #print(data)
                    for i in range(len(module['data_type'])):
                        message = self.base_metric_path + module['module_name'] + '.' + module['data_type'][i] + ' ' + str(data[i]) + ' ' + timestamp + '\n' 
                        #print(message)
                        sock.sendall(message.encode())
        sock.close()
        self.last_datafetch = te_time
        
    def check_auth(self):
        if self.expires_in + 30 >= time.time():
            self.refresh_auth()
        else:
            pass
        
    def __init__(self):
        self.authenticate()
        self.authenticate()
        self.get_modules()

netatmo = netatmosync()

while True: 
    while netatmo.last_datafetch + datetime.timedelta(hours=12) < datetime.datetime.now():
        netatmo.check_auth()
        netatmo.sync_data()
        sleep(30)
        
    while netatmo.last_datafetch + datetime.timedelta(hours=24) > datetime.datetime.now():
        netatmo.check_auth
        if netatmo.last_datafetch + datetime.timedelta(minutes=netatmo.sync_delta + 2) < datetime.datetime.now():
            netatmo.sync_data(delta_hours=0, delta_minutes=netatmo.sync_delta)
        sleep(120)
        



