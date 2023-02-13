
''' Notas
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
acumulatedKWh = 0

'''datetime
https://docs.python.org/3/library/datetime.html#module-datetime

formato de la última lectura diccionario last_reading_d
{'year': 2022, 'month': 4, 'day': 6, 'hour': 20, 'minute': 0}
'''

'''Guardar la ultima lectura en reading_register_
se guarda en la posicion del listado que corresponde
reading_register_ se guarda al final del script para recuperarlo en la siguiente consulta
# reading_register_[n] ---> ["ES00XXXXXXXXXXXXXXXX0F": {"cif": "XXXXXXXXX", "energy": 33345345, "ultima": {"year": 2022, "month": 4, "day": 9, "hour": 22, "minute": 0}}},]
'''
def guardar_ultima_lectura(d, position_):
     cups_t_r = d['cups'] # cups to register
     last_t_r = {"year": d["year"], "month": d["month"], "day": d["day"], "hour": d["hour"], "minute": d["minute"]} # last time to register
     reading_register_[position_][cups_t_r]["ultima"] = last_t_r
     logging.debug('registro de la ultima lectura: '+ str(reading_register_[position_]))

def mqtt_tx(client,s_value):
    # logging.debug(client + "  " + s_register + "  " + s_value)
    # Parseo de las variables
    mqtt_topic_prefix = parser.get('mqtt_broker','mqtt_topic_prefix')
    mqtt_ip = parser.get('mqtt_broker','mqtt_ip')
    mqtt_login = parser.get('mqtt_broker','mqtt_login')
    mqtt_password = parser.get('mqtt_broker','mqtt_password')

    mqtt_auth = { 'username': mqtt_login, 'password': mqtt_password }
    response = publish.single(mqtt_topic_prefix + "/" + client, s_value, hostname=mqtt_ip, auth=mqtt_auth)
    # response : None
    # En el servidor: Client mosq-QQQQQaaaaaaaaaaaaa received PUBLISH (d0, q0, r0, m0, 'datadis/ESxxxxxxxxxxxxxxxxxx0F', ... (144 bytes))

''' Procesar la lectura antes de enviarla. Incluir acumulatedKWh
d viene con el formato:
{'cups': 'ES00XXXXXXXXXXXXXXXX0F', 'consumptionKWh': 0.023,'year': 2022, 'month': 5, 'day': 14, 'hour': 3, 'minute': 0}
rr_index: el índice que indica el registro del clinte del que se hace la lectura (indice de reading_register_)
necesario incluirle acumulatedKWh antes de enviarla

lectura procesada --> {'cups': 'ES00XXXXXXXXXXXXXXXX0F', 'consumptionKWh': 0.034, 'year': 2022, 'month': 5, 'day': 13, 'hour': 13, 'minute': 0}
mqtt ---> {'cups': 'ES00XXXXXXXXXXXXXXXX0F', 'consumptionKWh': 0.034, 'year': 2022, 'month': 5, 'day': 13, 'hour': 13, 'minute': 0, 'acumulatedKWh': 33345354.367}
'''
def procesar_lectura(d,rr_index_):
    logging.debug('lectura procesada --> ' + str(d))
    register = reading_register_[rr_index_]
    cupsQ = list(register.keys())[0]
    cifQ = register[cupsQ]["cif"]
    energy_0 = register[cupsQ]["energy"]

    # Incluir acumulatedKWh en el diccionario a enviar por MqTT
    d["acumulatedKWh"] =  round(energy_0 + d["consumptionKWh"] ,3)

    # Actualizar reading_register con el acumulado de energía
    reading_register_[rr_index_][cupsQ]["energy"] = d["acumulatedKWh"]

    logging.debug("mqtt ---> " + str(d))

    # Comentar esta linea para pruebas que no se envien al servidor
    mqtt_tx(d['cups'], str(d))

''' formato_time. Los datos recibidos desde Datadis:
{'cups': 'ES00XXXXXXXXXXXXXXXX0F', 'date': '2022/04/09', 'time': '18:00', 'consumptionKWh': 0.08, 'obtainMethod': 'Real'}
<class 'dict'>

Lo datos procesasdos devueltos
------------------------------
Son los datos en un diccionario plano mas la corrección de la fecha
{'cups': 'ES00XXXXXXXXXXXXXXXX0F', 'consumptionKWh': 0.841, 'year': 2022, 'month': 4, 'day': 10, 'hour': 0, 'minute': 0}
<class 'dict'>

También se devuelve un objeto datetime
'''
def formato_time(d):
     # https://codigofacilito.com/articulos/fechas-python
     year= int(d['date'].split("/")[0])
     month= int(d['date'].split("/")[1])
     day= int(d['date'].split("/")[2])
     hour= int(d['time'].split(":")[0])
     minutes = int(d['time'].split(":")[1])
     increase = 0
     if hour == 24:
         hour = 23
         increase = 1
     last_reading_o = datetime(year, month, day, hour, minutes, 00, 00000) + timedelta(hours=increase)
     # para trabajar en GMT
     # Open Energy Monitor trabaja en GMT
     last_reading_o = last_reading_o - timedelta(hours=2)
     last_reading_r = {'cups':d['cups'],
                       'consumptionKWh':d['consumptionKWh'],
                       'year':last_reading_o.year,
                       'month':last_reading_o.month,'day':last_reading_o.day,
                       'hour':last_reading_o.hour,
                       'minute':last_reading_o.minute
                       }
     return [last_reading_r,last_reading_o]

''' ver reading_register con formato json
cat registers/reading_register.txt | python -m json.tool
'''
def abrir_reading_register():
    rr_path = "registers/reading_register.txt"
    lectura=open(rr_path, "r", encoding="utf-8")
    data = json.load(lectura)
    lectura.close()
    return data

def save_reading_register(cups_register):
    rr_path = "registers/reading_register.txt"
    writing =open(rr_path, "w", encoding="utf-8")
    json.dump(cups_register,writing)
    writing.close()

'''comprobar_consulta
Se comprueba que tenga un numero de registros mínimo
Que los formatos son los correctos

En cada posicion del registro:
{"ES00XXXXXXXXXXXXXXXA0F": {"cif": "XXXXXXXXX", "energy": 33345345, "ultima": {"year": 2022, "month": 5, "day": 13, "hour": 0, "minute": 0}}}
<class 'dict'>

last_datetime_r_d
decha en formato datetime.datetime para enviar los datos por mqtt a partir de ese punto

logging.debug(type(data_))
<class 'list'>

Diccionario enviado por mqtt:
{'cups': 'ES00XXXXXXXXXXXXXXXA0F', 'consumptionKWh': 0.038, 'acumulatedKWh': 29.449, 'year': 2022, 'month': 5, 'day': 12, 'hour': 12, 'minute': 0}
'''
def comprobar_consulta(data_, rr_position, last_datetime_r_d_):  #reading register position, objeto datetime.datetime
    n = len(data_)
    last_datetime_valid = last_datetime_r_d_
    if((type(data_) == type(list())) and (n > 4)):
        try:
            # Calcular la hora hasta la que llegan los datos (llegan ceros al final)
            for x in data_:
                if(x['consumptionKWh'] != 0):
                    power_data = formato_time(x)
                    logging.debug("tiempo del valor: " +str(power_data[1]) + " vs " + str(last_datetime_r_d_))
                    if(power_data[1] > last_datetime_r_d_):
                        last_datetime_valid = power_data[1]
            logging.info("PROCESAR DESDE --> " + str(last_datetime_r_d_))
            logging.info("PROCESAR HASTA --> " + str(last_datetime_valid))

            # Procesar el rango de datos validos
            valid_power_data = {}
            for y in data_:
                power_data = formato_time(y)
                logging.debug("tiempo del valor recibido --> " +str(power_data[1]))
                if((power_data[1] > last_datetime_r_d_) and (power_data[1] <= last_datetime_valid)):
                    procesar_lectura(power_data[0],rr_position)
                    valid_power_data = power_data[0]

            if(valid_power_data != {}):
                guardar_ultima_lectura(valid_power_data, rr_position)
                logging.debug("Ultimo dato procesado:  " + str(power_data[0]))
            else:
                logging.info("Sin nuevos datos para procesar ")


        except:
            logging.warning("Error al procesar los datos ---> data_")

'''
Para pruebas,
evita la consulta cada vez que se ejecuta el script
Los tipos:
logging.debug(type(data)) #<class 'list'>
logging.debug(type(data[i])) #<class 'dict'>
'''
def abrir_lectura():
    lectura=open("cooperase_response.txt", "r", encoding="utf-8")
    data = json.load(lectura)
    lectura.close()
    return data

def formato_lectura(text):
    try:
        data = json.loads(text)
    except:
        data = []
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

    cupsQ = list(x.keys())[0]
    logging.debug(cupsQ)

    cifQ = x[cupsQ]["cif"]
    distributorCodeQ = x[cupsQ]["distributorCode"]
    pointTypeQ = x[cupsQ]["pointType"]

    # Decision de las fechas del query
    last_date_r = x[cupsQ]["ultima"] # last date registered
    # ultima fecha del registro en formato datetime
    last_date_r_d = date(last_date_r["year"],
                         last_date_r["month"],
                         last_date_r["day"])
    last_datetime_r_d = datetime(last_date_r["year"],
                                 last_date_r["month"],
                                 last_date_r["day"],
                                 hour=last_date_r["hour"])
    logging.debug("last_datetime_r_d ---> " + str(last_datetime_r_d) )

    end_date_d = date.today()
    # límite de la fecha inicial para la consulta 100 dias
    delta= timedelta(days=100)
    star_date_d = last_date_r_d
    if(last_date_r_d + delta <= end_date_d):
        star_date_d = end_date_d - delta # star date datetime(format)

    # Damos formato a startDateQ
    # Se parte de star_date_d
    star_day_str = str(star_date_d.day)
    if (star_date_d.day <= 9):
        star_day_str = "0" + str(star_date_d.day)
    star_month_str = str(star_date_d.month)
    if (star_date_d.month <= 9):
        star_month_str = "0" + str(star_date_d.month)
    # startDateQ = str(star_date_d.year) + "/" + star_month_str + "/" + star_day_str
    startDateQ = str(star_date_d.year) + "/" + star_month_str

    # Damos formato a endDateQ
    # Se parte de end_date_d
    end_day_str = str(end_date_d.day)
    if (end_date_d.day <= 9):
        end_day_str = "0" + str(end_date_d.day)
    end_month_str = str(end_date_d.month)
    if (end_date_d.month <= 9):
        end_month_str = "0" + str(end_date_d.month)
    # endDateQ = str(end_date_d.year) + "/" + end_month_str + "/" + end_day_str
    endDateQ = str(end_date_d.year) + "/" + end_month_str


    url = "http://datadis.es/api-private/api/get-consumption-data?authorizedNif="
    url += cifQ
    url += "&cups="
    url += cupsQ
    url += "&distributorCode="
    url += distributorCodeQ
    url += "&startDate="
    url += startDateQ
    url += "&endDate="
    url += endDateQ
    url += "&measurementType=0&pointType="
    url += pointTypeQ

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
    return [response_text,last_datetime_r_d]

def pedir_nuevo_key():
    logging.debug('El Key no se ha obtenido hoy. Pedimos un nuevo key')
    datadis_login = parser.get('datadis','datadis_login')
    datadis_password = parser.get('datadis','datadis_password')

    conn = http.client.HTTPSConnection("datadis.es")
    payload =  "username="
    payload += datadis_login
    payload += "&password="
    payload += datadis_password
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
    response_txt = response[0]  # La respuesta en texto plano
    last_datetime_r_d = response[1] # ultima fecha leida tipo datetime.datetime
    logging.debug("++++response_txt: ")
    logging.debug(response_txt)
    # logging.debug(type(response_txt))# <class 'str'>
    data_red = formato_lectura(response_txt)# devuelve la lectura en formato json
    comprobar_consulta(data_red,rr_index,last_datetime_r_d) # Hace la comprobación y la procesa

''' Guarda todos los registros de lectura
de todos los usuarios en un fichero
los registros se han ido actualizando en cada bucle
'''
# Comentar esta linea para probar sin que se registre
save_reading_register(reading_register_)