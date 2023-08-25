
''' Notas
vie 25 ago 2023 12:47:22 CEST

tipos de varialbles en python
https://www.codigofuente.org/variables-en-python/
Para ver lo que se esta enviando al servidor
mosquitto_sub -d -h  data-test.endef.com -p 1883 -u xxxxxxx -P xxxxxxxx -t "datadis/#"
'''
#!/usr/bin/env python
import configparser
import paho.mqtt.publish as publish
import json
from datetime import datetime
from datetime import timedelta
from datetime import date
# import pickle
import http.client
import os
import time
import requests
import logging
from logging.handlers import RotatingFileHandler


''' Niveles de logging
Para obtener _TODO_ el detalle: level=logging.DEBUG
Para comprobar los posibles problemas level=logging.WARNINg
Para comprobar el funcionamiento: level=logging.INFO
'''
logging.basicConfig(
        level=logging.DEBUG,
        handlers=[RotatingFileHandler('./logs/log_datadis.log', maxBytes=10000000, backupCount=4)],
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p')

parser = configparser.ConfigParser()

''' ver reading_register con formato json
cat registers/reading_register.txt | python -m json.tool
'''
def abrir_reading_register():
    rr_path = "registers/reading_register.txt"
    lectura=open(rr_path, "r", encoding="utf-8")
    data = json.load(lectura)
    lectura.close()
    return data


'''datos necesarios para la consulta
---------------------------------
cupsQ="ES00311041XXXXXXXXXX0F
cifQ = "XXXXXXX4B
startDateQ="2022/04/06"
endDateQ="2022/04/09"
'''
def consulta_de_consumos(x):
    logging.debug("++++ Inicio de la consulta de consumos")

    cifQ = x["cif"]
    distributorCodeQ = x["distributorCode"]

    # url = "http://datadis.es/api-private/api/get-consumption-data?authorizedNif="
    url = "https://datadis.es/api-private/api/get-supplies?authorizedNif="
    url += cifQ
    url += "&distributorCode="
    url += distributorCodeQ

    logging.info(url)

    # Consulta de los consumos
    payload={}

    # key_path = "registers/temporal_key.txt"
    key_file_open=open(key_path, "r", encoding="utf-8")
    key_file_red = key_file_open.read()
    key_file_open.close()
    logging.debug(key_file_red)

    headers = {  'Authorization': 'Bearer ' + key_file_red
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    response_text = response.text
    return response_text

def pedir_nuevo_key():
    logging.debug('El Key no se ha obtenido hoy. Pedimos un nuevo key')
    datadis_login = parser.get('datadis','datadis_login')
    datadis_password = parser.get('datadis','datadis_password')

    conn = http.client.HTTPSConnection("datadis.es")
    payload =  "username="
    payload += datadis_login
    payload += "&password="
    payload += datadis_password
    logging.debug(payload)
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
            }
    conn.request("POST", "/nikola-auth/tokens/login", payload, headers)
    res = conn.getresponse()
    data = res.read()
    logging.debug(data.decode("utf-8"))
    key = data.decode("utf-8")
    key_f=open(key_path, "w", encoding="utf-8")
    key_f.write(key)
    key_f.close()


'''Si el key no es de hoy pido un nuevo key
date.today()
<class 'datetime.date'>
'''
def obtener_key():
    try:
        m_time = os.path.getmtime(key_path)
        # logging.debug(type(m_time)) # <class 'float'>
    except:
        m_time =1.1
    # logging.debug('time_m: ' + str (m_time))

    today = date.today()
    m_file_time = date.fromtimestamp(m_time)
    # logging.debug('m_file_time: ')
    # logging.debug(m_file_time) # 1970-01-01
    # logging.debug(type(m_file_time)) # <class 'datetime.date'>
    if (date.today() != m_file_time):
        pedir_nuevo_key()


#************************
#** LOGICA DE PROCESO ***
#************************
# mqtt y credenciales de datadis
parser.read('config_datadis.ini')

key_path = "registers/temporal_key.txt"

obtener_key()

'''Cada x en reading_register_
-----------------------------
reading_register es un fichero con los datos de cada usuario que incluye los datos de la última lectura valida
El formato es: Lista de diccionarios
cada elemento del listado, "x":
{"ES00XXXXXXXXXXXXXXXX0F": {"cif": "XXXXXXX4B", "energy": 146.747, "ultima": {"year": 2022, "month": 6, "day": 18, "hour": 16, "minute": 0}}}
<class 'dict'>
'''
reading_register_ = abrir_reading_register()

for x in reading_register_:
    rr_index = reading_register_.index(x) #reading_register_ index
    response = consulta_de_consumos(x)
    logging.debug("++++response_txt: ")
    logging.debug(response)
    # logging.debug(type(response_txt))# <class 'str'>
    # data_red = formato_lectura(response_txt)# devuelve la lectura en formato json

''' Guarda todos los registros de lectura
de todos los usuarios en un fichero
los registros se han ido actualizando en cada bucle
'''
# Comentar esta linea para probar sin que se registre
# save_reading_register(reading_register_)