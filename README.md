# Consulta periódica a Datadis
Este script consulta, a la plataforma de Datadis, los consumos que se han realizado de un CUPS concreto o una lista de ellos

Para hacer la consulta se ha de tener las credenciales correspondientes que se guardan en el fichero **config_datadis.ini**. Se incluye un fichero de ejemplo que es necesario sustituir

El resultado se transmite por MqTT a un servidor. La configuración se incluyen también en el fichero config_datadis.ini

El escript guarda los valores de energía y las fechas de las lecturas ya hechas en el fichero **reading_register.txt** del que se incluye un ejemplo. Este fichero se toma como referencia y por lo tanto permite cambiar los valores iniciales de la siguiente ejecución del script

El script esta preparado para ejecutarse una vez al día para lo que hay que configurar una línea en el crontab en el servidor de trabajo
