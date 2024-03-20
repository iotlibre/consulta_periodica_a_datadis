
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
import mysql.connector


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

'''
Guardar la FECHA de la ultima lectura en reading_register_
se guarda en la posicion del listado que corresponde
reading_register_ se guarda al final del script para recuperarlo en la siguiente consulta
reading_register_[n] ---> 
    [{"ES0031104116371034RA0F": {"cif": "X5483924B", "energy": 3465.756, "distributorCode": "2", 
    "pointType": "5", "ultima": {"year": 2024, "month": 3, "day": 2, "hour": 22, "minute": 0}}}]
'''
def registrar_ultima_fecha(d, position_):
     cups_t_r = d['cups'] # cups to register
     last_t_r = {"year": d["year"], "month": d["month"], "day": d["day"], "hour": d["hour"], "minute": d["minute"]} # last time to register
     reading_register_[position_][cups_t_r]["ultima"] = last_t_r
     logging.debug('registro de la ultima lectura: '+ str(reading_register_[position_]))

'''
topic: prefijo + cliente 
value: {'cups': 'ES00XXXXXXXXXXXXXXXX0F', 'consumptionKWh': 0.034, 'year': 2022, 'month': 5, 'day': 13, 'hour': 13, 'minute': 0, 'acumulatedKWh': 33345354.367}

'''
def mqtt_tx(client,s_value):
    # logging.debug(client + "  " + s_register + "  " + s_value)
    # Parseo de las variables
    mqtt_topic_prefix = parser.get('mqtt_broker','mqtt_topic_prefix')
    mqtt_ip = parser.get('mqtt_broker','mqtt_ip')
    mqtt_login = parser.get('mqtt_broker','mqtt_login')
    mqtt_password = parser.get('mqtt_broker','mqtt_password')

    mqtt_auth = { 'username': mqtt_login, 'password': mqtt_password }
    response = publish.single(mqtt_topic_prefix + "/" + client, s_value, hostname=mqtt_ip, auth=mqtt_auth)

def db_ioe_tx(ioe_d, id_c_p ):  # ioe dictionary  id_consumer_profile

    conexion0=mysql.connector.connect(host = parser.get('ioe_db','db_ip'),
                                      user = parser.get('ioe_db','db_login'),
                                      passwd = parser.get('ioe_db','db_password'),
                                      database = parser.get('ioe_db','database'))

    cursor0=conexion0.cursor()

    sql_str = "INSERT INTO consumer_profile_consumption(id_consumer_profile, year, month, day, hour, consumption) VALUES("
    sql_str += str(id_c_p) + ","
    sql_str += str(ioe_d['year'])+ ","
    sql_str += str(ioe_d['month'])+ ","
    sql_str += str(ioe_d['day'])+ ","
    sql_str += str(ioe_d['hour'])+ ","
    sql_str += str(ioe_d['consumptionKWh'])+ ")"

    logging.debug("sql_str: " + sql_str)
    cursor0.execute(sql_str)
    logging.info("Record Inserted: " + str(cursor0.rowcount))

    conexion0.commit()

    conexion0.close()  



''' Procesar la lectura antes de enviarla. Incluir acumulatedKWh
d viene con el formato:
{'cups': 'ES00XXXXXXXXXXXXXXXX0F', 'consumptionKWh': 0.023,'year': 2022, 'month': 5, 'day': 14, 'hour': 3, 'minute': 0}
rr_index: el índice que indica el registro del clinte del que se hace la lectura (indice de reading_register_)
necesario incluirle acumulatedKWh antes de enviarla

lectura procesada --> {'cups': 'ES00XXXXXXXXXXXXXXXX0F', 'consumptionKWh': 0.034, 'year': 2022, 'month': 5, 'day': 13, 'hour': 13, 'minute': 0}
mqtt ---> {'cups': 'ES00XXXXXXXXXXXXXXXX0F', 'consumptionKWh': 0.034, 'year': 2022, 'month': 5, 'day': 13, 'hour': 13, 'minute': 0, 'acumulatedKWh': 33345354.367}
'''
def procesar_lectura(d,rr_index_):
    logging.info('lectura procesada --> ' + str(d))
    register = reading_register_[rr_index_]
    cupsQ = list(register.keys())[0]
    energy_0 = register[cupsQ]["energy"]

    # Incluir acumulatedKWh en el diccionario a enviar por MqTT
    d["acumulatedKWh"] =  round(energy_0 + d["consumptionKWh"] ,3)

    # Actualizar reading_register con el acumulado de energía
    reading_register_[rr_index_][cupsQ]["energy"] = d["acumulatedKWh"]

    logging.info("mqtt ---> " + str(d))

    # Comentar esta linea para pruebas que no se envien al servidor
    mqtt_tx(d['cups'], str(d))


''' formato_consumo_ioe. Los datos recibidos desde Datadis:
{'cups': 'ES00XXXXXXXXXXXXXXXX0F', 'date': '2022/04/09', 'time': '18:00', 'consumptionKWh': 0.08, 'obtainMethod': 'Real'}
<class 'dict'>

Lo datos procesasdos devueltos
------------------------------
Son los datos en un diccionario plano mas la corrección de la fecha
{'cups': 'ES00XXXXXXXXXXXXXXXX0F', 'consumptionKWh': 0.841, 'year': 2022, 'month': 4, 'day': 10, 'hour': 0, 'minute': 0}
<class 'dict'>

También se devuelve un objeto datetime
'''
def formato_consumo_ioe(d):
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

    # reading_time_o           reading time object
    # reading_consumption_j    reading consumption json
    
    reading_time_o = datetime(year, month, day, hour, minutes, 00, 00000) + timedelta(hours=increase)
    # para trabajar en GMT
    # Open Energy Monitor trabaja en GMT
    reading_time_o = reading_time_o - timedelta(hours=2)
    reading_consumption_j = {'cups':d['cups'],
                                'consumptionKWh':d['consumptionKWh'],
                                'year':reading_time_o.year,
                                'month':reading_time_o.month,'day':reading_time_o.day,
                                'hour':reading_time_o.hour,
                                'minute':reading_time_o.minute
                                }
     
    return [reading_consumption_j,reading_time_o]

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
Que los formatos son los correctos
Que tenga un numero de registros mínimo para evitar las respuesta de error

En cada posicion del registro:
{"ES00XXXXXXXXXXXXXXXX0F": {"cif": "X11111111", "energy": 6185.915, "distributorCode": "2", "pointType": "5", 
"ultima": {"year": 2024, "month": 3, "day": 5, "hour": 22, "minute": 0}}}
<class 'dict'>

logging.debug(type(data_))
<class 'list'>

Diccionario enviado por mqtt:
{'cups': 'ES00XXXXXXXXXXXXXXXA0F', 'consumptionKWh': 0.038, 'acumulatedKWh': 29.449, 'year': 2022, 'month': 5, 'day': 12, 'hour': 12, 'minute': 0}
'''

def seleccionar_rango_registros(rr_position, data_):
    '''
    Selecciona el rango de registros valido de la respuesta de DATADIS
    inicio: obtenido de reading register
    '''
    logging.debug("++++ def_seleccionar_rango_registros")
    n = len(data_)

    rr = reading_register_[rr_position]

    register_data = get_register_data(rr)

    # last_r_dt_o
    last_datetime_r_d_ = register_data[1]

    last_datetime_valid = last_datetime_r_d_

    logging.info("type_data_: " + str(type(data_)))
    logging.info("n: " + str(n))

    if((type(data_) == type(list())) and (n > 8)):
        try:
            # Calcular la hora hasta la que llegan los datos (llegan ceros al final)
            for x in data_:
                if(x['consumptionKWh'] != 0):
                    power_data = formato_consumo_ioe(x)
                    logging.debug("tiempo del valor: " +str(power_data[1]) + " vs " + str(last_datetime_r_d_))
                    #2024-02-15 12:00:00 vs 2024-01-02 22:00:00
                    if(power_data[1] > last_datetime_r_d_):
                        last_datetime_valid = power_data[1]
            logging.debug("PROCESAR DESDE --> " + str(last_datetime_r_d_))
            logging.debug("PROCESAR HASTA --> " + str(last_datetime_valid))
        except:
            logging.warning("Error al procesar los datos ---> data_")

    logging.info("PROCESAR DESDE --> " + str(last_datetime_r_d_))
    logging.info("PROCESAR HASTA --> " + str(last_datetime_valid))
    return [last_datetime_r_d_ , last_datetime_valid]


def seleccionar_procesar_registros(rr_position, data_, time_range):  

    valid_power_data = {}
    for y in data_:
        power_data = formato_consumo_ioe(y)
        logging.debug("tiempo del valor recibido --> " +str(power_data[1]))

        if((power_data[1] > time_range[0]) and (power_data[1] <= time_range[1])):
            procesar_lectura(power_data[0],rr_position)
            valid_power_data = power_data[0]


    if(valid_power_data != {}):
        registrar_ultima_fecha(valid_power_data, rr_position)
        logging.debug("Ultimo dato procesado:  " + str(power_data[0]))
    else:
        logging.info("Sin nuevos datos para procesar ")


def write_DB(rr_position, data_, time_range):

    '''
    reading register position, 
    Procesar el rango de datos validos
    id_c_p será el mismo para todo un registro del reading_register
    '''

    # Parsear los datos de la DB
    # consulta de id_consumer_profile a la db
    id_c_p = 0
    register = reading_register_[rr_position]
    cupsR = list(register.keys())[0]
    conexion0=mysql.connector.connect(host = parser.get('ioe_db','db_ip'),
                                    user = parser.get('ioe_db','db_login'),
                                    passwd = parser.get('ioe_db','db_password'),
                                    database = parser.get('ioe_db','database'))

    cursor0=conexion0.cursor()
    # Preguntarmos por el id del consumer 'XX003XXXX11637103XXXXX'
    sql_str = "SELECT id_consumer_profile FROM consumer_profile WHERE description = '"
    sql_str += str(cupsR) + "'"

    logging.info("sql_str: " + sql_str)

    cursor0.execute(sql_str)
    result_l = cursor0.fetchall() # result list
    logging.info(str(type(result_l)))
    # result_l <class 'list'>
    # result_l[0] <class 'tuple'>
    # result_l[0][0] <class 'int'>

    id_c_p = result_l[0][0]
    logging.info("id_consumer_profile: " + str(id_c_p))

    # conexion0.close()  

    '''
    INSERT INTO consumer_profile_consumption(id_consumer_profile, year, month, day, hour, consumption) VALUES(15,2024,2,28,0,0.31)
    '''
    sql = "INSERT INTO consumer_profile_consumption(id_consumer_profile, year, month, day, hour, consumption) VALUES(%s,%s,%s,%s,%s,%s)"
    val = []
    for y in data_:
        power_data = formato_consumo_ioe(y)
        logging.debug("tiempo del valor recibido --> " +str(power_data[1]))

        if((power_data[1] > time_range[0]) and (power_data[1] <= time_range[1])):
         
            val.append ((str(id_c_p),
                         str(power_data[0]['year']),
                         str(power_data[0]['month']),
                         str(power_data[0]['day']),
                         str(power_data[0]['hour']),
                         str(power_data[0]['consumptionKWh'])))
            
            logging.debug(str(val[len(val)-1]))

    if(val != []):
        logging.debug("Ultimo dato procesado:  " + str(power_data[0]))
        # cursor0=conexion0.cursor()
        cursor0.executemany(sql, val)
        conexion0.commit()

    else:
        logging.info("Sin nuevos datos para procesar ")
    
  
    logging.info("was inserted: " + str(cursor0.rowcount)) 
    conexion0.close()  




def abrir_lectura(link):
    '''
    evita la consulta cada vez que se ejecuta el script
    logging.debug(type(data)) #<class 'list'>
    logging.debug(type(data[i])) #<class 'dict'>
    '''
    lectura=open(link, "r", encoding="utf-8")
    # data = json.load(lectura)
    data = lectura.read()
    lectura.close()
    return data

def formato_lectura(text):
    try:
        data = json.loads(text)
    except:
        data = []
    return data


def guardar_en_archivo(t, rr_index):
    '''
    Guarda texto en un fichero
    Lo guarda en el directorio logs
    Para el nombre del fichero necesita el cups y el año
    Lo obtiene de reading_register mediante el indice
    '''
    logging.info('inicio def_guardar_archivo')
    logging.debug('archivo:  ' + str(t))
    
    register = reading_register_[rr_index]
    cupsQ = list(register.keys())[0]
    year = register[cupsQ]["ultima"]["year"]

    file_path = "logs/"
    file_path += cupsQ
    file_path += "_"
    file_path += str(year)
    file_path += ".txt"

    logging.info('file_path --> ' + file_path)
    
    f = open(file_path, 'w')
    f.write(t)
    f.close()
    


def get_register_data(r):
    logging.debug("++++ Inicio def_get_register_data")

    cupsQ = list(r.keys())[0]
    logging.debug(cupsQ)

    # Decision de las fechas del query
    last_date_r = r[cupsQ]["ultima"] # last date registered

    # ultima fecha del registro en formato datetime
    # last_red_date_objet  last_red_datetime_object
    last_r_da_o = date(last_date_r["year"],
                       last_date_r["month"],
                       last_date_r["day"])
    last_r_dt_o = datetime(last_date_r["year"],
                           last_date_r["month"],
                           last_date_r["day"],
                           hour=last_date_r["hour"])
    
    # inicio_anual red date annual init object
    # da_a_i_o data annual init objet

    da_a_i_o = date(last_date_r["year"],1,1)
    da_a_f_o = date.today()    
    delta= timedelta(days=365)
    
    if(da_a_i_o + delta <= da_a_f_o):
        da_a_f_o = date(last_date_r["year"],12,31)

    # final_anual
    logging.debug("last_r_da_o ---> " + str(last_r_da_o))
    logging.debug("last_r_dt_o ---> " + str(last_r_dt_o))
    logging.debug("da_a_i_o ------> " + str(da_a_i_o))
    logging.debug("da_a_f_o ------> " + str(da_a_f_o))

    return [last_r_da_o,last_r_dt_o,da_a_i_o,da_a_f_o]

def consulta_de_consumos(x,annual=False):
    '''
    annual = True consulta el año de la fecha de reading_register.txt
    retorna todo el resultado de la consulta
    Procesa hacia DB o MQTT *desde* la fecha(datetime.datetime) de reading_register.txt
    '''
    logging.info("++++++ Inicio def_consulta_de_consumos")

    cupsQ = list(x.keys())[0]
    logging.debug(cupsQ)

    cifQ = x[cupsQ]["cif"]
    distributorCodeQ = x[cupsQ]["distributorCode"]
    pointTypeQ = x[cupsQ]["pointType"]
    
    # Decision de las fechas del query
    register_data = get_register_data(x)
    last_date_r_d = register_data[0]
    last_datetime_r_d = register_data[1]
 
    end_date_d = date.today()
    # límite de la fecha inicial para la consulta 100 dias
    delta= timedelta(days=100)
    star_date_d = last_date_r_d
    if(last_date_r_d + delta <= end_date_d):
        star_date_d = end_date_d - delta # star date datetime(format)

    # cuando la consulta es anual
    if (annual):
        star_date_d = register_data[2]
        end_date_d = register_data[3] 
        logging.debug("consulta_de_consumos--annual ---> [2]" + str(star_date_d) +" [3]" + str(end_date_d) )

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

    response_text = "formato de inicio"

    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response_text = response.text

    except:
        response_text = "error en respuesta de DATADIS"

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



def obtener_key():
    '''Si el key no es de hoy pido un nuevo key
    date.today()
    <class 'datetime.date'>
    '''
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
        
'''
(0) guardar consulta en archivo
(1) guardar consulta anual en archivo
(2) a partir de archivo tx datos
(3) a partir de consulta tx datos
(4) hacer consulta parcial y enviarla a emoncms (por mqtt)

'''

# mqtt y credenciales de datadis
parser.read('config_datadis.ini')
key_path = "registers/temporal_key.txt"

#(0)(1)(3)(2)
obtener_key()

'''Cada x en reading_register_
-----------------------------
reading_register es un fichero con los datos de cada usuario que incluye los datos de la última lectura valida
El formato es: Lista de diccionarios
cada elemento del listado, "x":
En cada posicion del registro:
{"ES00XXXXXXXXXXXXXXXX0F": {"cif": "X11111111", "energy": 6185.915, "distributorCode": "2", "pointType": "5", 
"ultima": {"year": 2024, "month": 3, "day": 5, "hour": 22, "minute": 0}}}
<class 'dict'>
'''
reading_register_ = abrir_reading_register()



for x in reading_register_:

    rr_index = reading_register_.index(x) #reading_register_ index
    

    #(0)(3)(4)     
    response_txt = consulta_de_consumos(x,False)

    #(2)
    #response_txt = abrir_lectura("logs/ES0031103236677001PB0F_2023.txt")

    logging.debug("++++response_txt: ")
    logging.debug(response_txt)

    #(0)(1)
    # guardar_en_archivo(response_txt, rr_index)

    #(0)(1)(2)(3)(4)
    # logging.debug(type(response_txt))# <class 'str'>
    data_red = formato_lectura(response_txt)# devuelve la lectura en formato json

    #(0)(1)(2)(3)(4)
    # devuelve: [last_datetime_r_d_ , last_datetime_valid]
    rango_registros = seleccionar_rango_registros(rr_index, data_red)

    # selecciona los registros del rango valido
    # procesa los registros: calcula acumulatedKWh tx mqtt insert db
    seleccionar_procesar_registros(rr_index, data_red, rango_registros)

    # selecciona los registros del rango valido
    # Inserta los registros en la base de datos
    #write_DB(rr_index, data_red, rango_registros)



''' Guarda todos los registros de lectura
de todos los usuarios en un fichero
los registros se han ido actualizando en cada bucle
'''
# Comentar esta linea para probar sin que se registre
save_reading_register(reading_register_)