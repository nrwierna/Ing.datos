# Cuarta entrega - Proyecto Final
import pandas as pd
import requests  #realiza solicitudes HTTP - hacer una solicitud GET a una API pública y obtener los datos en formato JSON.
import psycopg2  # interactuar con bases de datos PostgreSQL. Conectarse a una base de datos Amazon Redshift (que está basada en PostgreSQL) 
                 #y realizar operaciones de base de datos como crear tablas e insertar datos.
import smtplib

#import sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

#import airflow
#from airflow import DAG
#from airflow.operators.python_operator import PythonOperator

# hora inicio
print(f"inicio script: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
#variable Status finalizacion ETL bandera error 0 sin error - 1 con error
eerror=0

# Configuración de Redshift
redshift_host = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com" #"HOST_DE_REDSHIFT"
redshift_db = "data-engineer-database"   #"NOMBRE_DE_LA_BASE_DE_DATOS"
redshift_user = "nrwnet_coderhouse"  #"USUARIO_DE_REDSHIFT"
redshift_password = "lR924mdYov"  #"CONTRASEÑA_DE_REDSHIFT"
redshift_port = "5439"     #"PUERTO_DE_REDSHIFT"

# Conexión a Redshift
conn = psycopg2.connect(
    host=redshift_host,
    dbname=redshift_db,
    user=redshift_user,
    password=redshift_password,
    port=redshift_port
)
cursor = conn.cursor()

# configuracion API
# URL de la API de RapidAPI para obtener el pronóstico
api_url = 'https://weatherapi-com.p.rapidapi.com/history.json'
# Cabeceras requeridas para la API de RapidAPI
headers = {
	"X-RapidAPI-Key": "6ab9bce469msh4228a762e86ee73p1cd9e0jsn4449ff6aa519",
	"X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
}


# borrar todos los registros de la tabla para volver a cargar los mismos actualizados 
#delete_query = "DELETE FROM pronostico_clima;"
# Ejecutar la sentencia
#cursor.execute(delete_query)
# Confirmar la transacción
#conn.commit()

# hora borrado
#print(f"borrado tabla: {datetime.now()}")

#eliminar la tabla 
#drop_table_query = "DROP TABLE IF EXISTS pronostico_clima;"
# Ejecutar la sentencia
#cursor.execute(drop_table_query)
# Confirmar la transacción
#conn.commit()
#print(f"eliminacion tabla: {datetime.now()}")


# Configuración del servidor SMTP para mail
smtp_server = 'smtp.gmail.com'
puerto = 587  #  tls
usuario = 'nrwnett@gmail.com'
contraseña = 'xxxxxxxxxxxx'

# Configuración del mensaje
de = usuario
para = 'nrwnett@gmail.com'
asunto = 'Status ETL_pronostico'

# Cuerpo del mensaje
mensaje = MIMEMultipart()
mensaje['From'] = de
mensaje['To'] = para
mensaje['Subject'] = asunto

#Crear tabla en Redshift
create_table_query = """
CREATE TABLE IF NOT EXISTS pronostico_clima(
            id_registro         varchar,
            ciudad              varchar,
            region              varchar,
            pais                varchar,
            latitud             varchar,
            longitud            varchar,
            descripcion_clima   VARCHAR,
            humedad             VARCHAR,
            fecha               VARCHAR,
            temperatura_minima  VARCHAR,
            temperatura_maxima  VARCHAR,
            velocidad_viento    VARCHAR,
            direccion_viento    VARCHAR,
            presion_atmosferica VARCHAR, 
            indice_uv           VARCHAR, 
            visibilidad         VARCHAR, 
            precipitacion       VARCHAR,
            fecha_alta_proceso  varchar
);
 """

#################  LOGS ################
#create_table_logs = """
#CREATE TABLE IF NOT EXISTS pronostico_logs(
#            id_log              varchar,
#            fecha_alta_proceso  VARCHAR,
#            id_msj              VARCHAR,
#            msj                 VARCHAR
#);
# """
# Ejecutar la consulta
#cursor.execute(create_table_logs)
#conn.commit()

#############################################

id_log=0
fecha_alta_proceso=datetime.now().date()
id_msj=0
msj=''

def insertar_logs(cursor, conn, id_log, fecha_alta_proceso, id_msj, msj):
    insert_query_logs = """
    INSERT INTO pronostico_logs (
        id_log,
        fecha_alta_proceso,
        id_msj,
        msj
    ) VALUES (%s, %s, %s, %s);
    """
    # Ejecutar la consulta de inserción
    cursor.execute(insert_query_logs, (id_log, fecha_alta_proceso, id_msj, msj))
    # Confirmar la transacción
    # conn.commit()
 
###########################################################################
# Luego, ejecuta la consulta de inserción con todos los valores
id_log = "1"
id_msj = "123"
msj = "Este es un mensaje de ejemplo"

insertar_logs(cursor, conn, id_log, fecha_alta_proceso, id_msj, msj)
conn.commit()
###########################################################################


# Nombre de la tabla que quieres crear
nombre_tabla = "pronostico_clima"

# Consulta SQL para verificar si la tabla ya existe
check_table_query = f"""
SELECT EXISTS (
   SELECT 1
   FROM   information_schema.tables 
   WHERE  table_name = '{nombre_tabla}'
);
"""

# Ejecutar la consulta
cursor.execute(check_table_query)

# Obtener el resultado de la consulta
tabla_existe = cursor.fetchone()[0]

# Verificar el resultado y obtener el valor del registro_id para almacenar
if tabla_existe:
    print(f"La tabla '{nombre_tabla}' ya existe.")
    # Obtener el valor máximo de id_registro
    max_id_query = "SELECT MAX(id_registro) FROM pronostico_clima;"
    # Ejecutar la consulta
    cursor.execute(max_id_query)
    # Obtener el resultado del máximo id_registro
    id_registro = cursor.fetchone()[0]

    # Agregar columna 'fecha_hoy' si no existe
    #alter_table_query = "ALTER TABLE pronostico_clima ADD COLUMN fecha_alta_proceso varchar;"
    #cursor.execute(alter_table_query)
    #conn.commit()
    # Actualizar registros existentes con la fecha actual
    #update_query = "UPDATE pronostico_clima SET fecha_alta_proceso = fecha;"
    #cursor.execute(update_query)
    #conn.commit()

    fecha_hoy = datetime.now().date()
    #fecha_resta = fecha_hoy - timedelta(days=1)
    # Convertir la fecha a una cadena de caracteres en el formato 'YYYY-MM-DD'
    #fecha_resta_str = fecha_resta.strftime('%Y-%m-%d')
    
    # Nombre de la tabla original y de respaldo
    tabla_original = "pronostico_clima"
    tabla_respaldo = "pronostico_clima_backup"

    # Crear la tabla de respaldo
    #sql_query = f"CREATE TABLE {tabla_respaldo} (LIKE {tabla_original});"
    #cursor.execute(sql_query)
    #conn.commit()

    # Verificar si ya existe un backup con la misma fecha
    sql_query = f"SELECT count(*) FROM {tabla_respaldo} WHERE fecha_backup = '{fecha_hoy}';"
    cursor.execute(sql_query)
    existe_backup = cursor.fetchone()[0]

    if existe_backup == 0:
        # Realizar la copia de seguridad solo si no existe un backup con la misma fecha
        sql_query = f"INSERT INTO {tabla_respaldo} SELECT *, '{fecha_hoy}' AS fecha_backup FROM {tabla_original};"
        #SELECT * FROM {tabla_original};"
        cursor.execute(sql_query)
        conn.commit()
        #print(f"Se realizó un backup el {fecha_hoy}.")
        insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(), f"Se realizó un backup el {fecha_hoy}.")

    else:
        #print(f"Ya existe un backup para la fecha {fecha_hoy}. No se realizó la operación.")
        insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(), f"Ya existe un backup para la fecha {fecha_hoy}. No se realizó la operación.")

else:
    id_registro = 0
    cursor.execute(create_table_query)
    # hora creacion tabla
    #print(f"creacion de tabla: {datetime.now()}")
    insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(), f"creacion de tabla: {datetime.now()}")    
    #print(f"Se creó la tabla '{nombre_tabla}'.")
    insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(), f"Se creó la tabla '{nombre_tabla}'.")
conn.commit()

 #Diccionario de datos de ciudades
cities_data = {
    'Buenos Aires':      '-34.61, -58.38',
    'Catamarca':         '-28.47, -65.79',
    'Córdoba':           '-31.41, -64.18',
    'Mendoza':           '-32.89, -68.84',
    'Rosario':           '-32.95, -60.65',
    'Salta':             '-24.79, -65.41',
    'Tucumán':           '-26.82, -65.22',
    'La Plata':          '-34.92, -57.95',
    'Santa Fe':          '-31.63, -60.70',
    'San Juan':          '-31.53, -68.52',
    'San Luis':          '-33.30, -66.34',
    'Neuquén':           '-38.95, -68.05',
    'Bahía Blanca':      '-38.71, -62.27',
    'Mar del Plata':     '-38.00, -57.56',
    'Tandil':            '-37.32, -59.14',
    'Misiones':          '-27.37, -55.90',
    'Chaco':             '-27.46, -58.98',
    'Formosa':           '-26.19, -58.17',
    'Jujuy':             '-24.19, -65.30',
    'Corrientes':        '-27.47, -58.84',
    'Santa Cruz':        '-51.62, -69.22',
    'Tierra del Fuego':  '-54.80, -68.30'
}

# Diccionario para almacenar los datos del clima por ciudad
cities_weather_data = {}

# Fechas para las que deseas obtener el pronóstico (puedes agregar más fechas)
# esta api posee la restriccion de 7 dias solamente de antiguedad
#fechas = ['2023-09-06','2023-09-07','2023-09-08','2023-09-09','2023-09-10','2023-09-11', '2023-09-12']

# Obtener la fecha de hoy
fecha_hoy = datetime.now().date()
# Crear una lista de fechas para los últimos 7 días
fechas = [(fecha_hoy - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]
print(fechas)
insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(),f"Fechas a analizar ' {fechas}'.")

# Itera a través de las ciudades y las fechas
for city_name, city_coords in cities_data.items():
    lat, lon = city_coords.split(', ')
    for fecha in fechas:
        # Consulta SQL para verificar si ya existe un registro para esa ciudad, fecha y coordenadas
        check_query = """
        SELECT COUNT(*) 
        FROM pronostico_clima 
        WHERE ciudad = %s AND fecha = %s AND latitud = %s AND longitud = %s;
        """
        cursor.execute(check_query, (city_name, fecha, lat, lon))
        cantidad_registros = cursor.fetchone()[0]
        
        if cantidad_registros > 0:
            #print(f"Ya existe un registro para {city_name} en la fecha {fecha}. No es necesario solicitarlo a la API.")
            insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(),f"Ya existe un registro para {city_name} en la fecha {fecha}. No es necesario solicitarlo a la API.")
        else:
            #print(f"No hay registros para {city_name} en la fecha {fecha}. Se solicitará a la API.")
            # Realizar la solicitud a la API y guardar los datos en la tabla
            #print(f"solicitud de datos: {datetime.now()}")

            # Itera a través de las ciudades en el diccionario
            #id_registro = 0
            #for city_name, city_coords in cities_data.items():
            #print("Aqui entra con" , city_name)
            #for fecha in fechas:
                #for fecha in fechas:
                #print ("Aqui la fecha es", fecha)
                #lat, lon = city_coords.split(',')
            querystring = {'q': f'{lat}, {lon}', 'dt': fecha, 'lang': 'es', 'units': 'metric'}

            # Realiza la solicitud a la API
            response = requests.get(api_url, headers=headers, params=querystring)

            # Comprueba si la solicitud fue exitosa (código de estado 200)
            if response.status_code == 200:
                data = response.json()
                # Extrae datos específicos del pronóstico
                #fecha_fecha= data['date']
                #temperatura_actual = data['current']['temp_c']
                # Incrementar el ID_registro
                id_registro = int(id_registro) + 1
                ciudad = city_name
                latitud = lat
                longitud = lon
                region = data['location']['region'] 
                pais = data['location']['country']
                descripcion_clima = data['forecast']['forecastday'][0]['day']['condition']['text']
                humedad = data['forecast']['forecastday'][0]['day']['avghumidity']
                temperatura_minima = data['forecast']['forecastday'][0]['day']['mintemp_c']
                temperatura_maxima = data['forecast']['forecastday'][0]['day']['maxtemp_c']
                velocidad_viento = data['forecast']['forecastday'][0]['day']['maxwind_kph']
                direccion_viento = 0
                presion_atmosferica = 0 
                indice_uv = data['forecast']['forecastday'][0]['day']['uv']
                visibilidad = data['forecast']['forecastday'][0]['day']['avgvis_km']
                precipitacion = data['forecast']['forecastday'][0]['day']['totalprecip_mm']

                # Almacena los datos del pronóstico en el diccionario
                cities_weather_data[city_name +' '+ fecha] = {
                    #'Temperatura actual': temperatura_actual,
                    'id_registro': id_registro,
                    'ciudad': ciudad,
                    'region': region,
                    'pais': pais,
                    'latitud': latitud,
                    'longitud': longitud,
                    'Descripción del clima': descripcion_clima,
                    'Humedad': humedad,
                    'Fecha': fecha,
                    'temperatura_minima': temperatura_minima,
                    'temperatura_maxima': temperatura_maxima,
                    'velocidad_viento': velocidad_viento ,
                    'direccion_viento': direccion_viento ,
                    'presion_atmosferica': presion_atmosferica, 
                    'indice_uv': indice_uv, 
                    'visibilidad' : visibilidad, 
                    'precipitacion': precipitacion 
                }

                # Consulta SQL para buscar si el registro ya existe
                select_query = "SELECT * FROM pronostico_clima WHERE ciudad = %s AND region = %s AND pais = %s AND fecha = %s"
                # Ejecutar la consulta con los parámetros correspondientes
                cursor.execute(select_query, (ciudad, region, pais, fecha))
                # Obtener los resultados de la consulta
                resultado = cursor.fetchone()
                # Si resultado no es None, significa que ya existe un registro con esos datos
                #if resultado is not None:
                #  print("El registro ya existe en la tabla.")
                #else:
                if resultado is None:
                    # Si resultado es None, puedes proceder a realizar la inserción
                    insert_query = """
                        INSERT INTO pronostico_clima(
                        id_registro,
                        ciudad,
                        region,
                        pais,
                        latitud,
                        longitud,
                        descripcion_clima,
                        humedad,
                        fecha,
                        temperatura_minima,
                        temperatura_maxima,
                        velocidad_viento,
                        direccion_viento,
                        presion_atmosferica,
                        indice_uv,
                        visibilidad,
                        precipitacion,
                        fecha_alta_proceso
                        ) VALUES (%s,%s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s);
                        """
                        # Luego, ejecuta la consulta de inserción con todos los valores
                    cursor.execute(insert_query, (
                        id_registro,
                        ciudad,
                        region,
                        pais,
                        latitud,
                        longitud,
                        descripcion_clima,
                        humedad,
                        fecha,
                        temperatura_minima,
                        temperatura_maxima,
                        velocidad_viento,
                        direccion_viento,
                        presion_atmosferica,
                        indice_uv,
                        visibilidad,
                        precipitacion,
                        fecha_hoy
                    ))
                
                conn.commit()

            else:
                eerror=1
                insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(),f'Error al conectarse a la API para la ciudad {city_name}. Código de estado:', response.status_code)
                #print(f'Error al conectarse a la API para la ciudad {city_name}. Código de estado:', response.status_code)
                
                # texto plano y/o HTML del cuerpo del mensaje
                texto = f"""Inicio Status ETL,
                Error al conectarse a la api,
                Fecha y hora {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                Fin Status ETL
                """
                mensaje.attach(MIMEText(texto, 'plain'))

                # Conectarse al servidor SMTP y enviar el correo
                try:
                    with smtplib.SMTP(smtp_server, puerto) as servidor:
                        servidor.starttls()
                        servidor.login(usuario, contraseña)
                        servidor.sendmail(de, para, mensaje.as_string())
                        print('Correo electrónico enviado correctamente.')
                        insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(),'Correo electrónico enviado correctamente.')
                except Exception as e:
                    print(f'Error al enviar el correo electrónico: {e}')
                    insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(),f'Error al enviar el correo electrónico: {e}')


# hora fin solicitud de datos
#print(f"Fin solicitud datos: {datetime.now()}")
insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(),f"Fin solicitud datos: {datetime.now()}")


# Imprime los datos del clima por ciudad
#for city_name, weather_data in cities_weather_data.items():
#    print(f'Ciudad: {city_name}')
#    for key, value in weather_data.items():
#        print(f'{key}: {value}')
#    print('-' * 40)

# hora fin impresion datos
#print(f"fin impresion: {datetime.now()}")

# Consulta SQL para contar los registros
count_query = "SELECT COUNT(*) FROM pronostico_clima;"
# Ejecutar la consulta
cursor.execute(count_query)
# Obtener el resultado del conteo
cantidad_registros = cursor.fetchone()[0]
# Mostrar el resultado en pantalla
#print(f"La tabla 'pronostico_clima' tiene {cantidad_registros} registros.")
insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(),f"La tabla 'pronostico_clima' tiene {cantidad_registros} registros.")



#enviar mail
if eerror==0: 
    # texto plano y/o HTML del cuerpo del mensaje
    texto = f"""Inicio Status ETL,
    Finalizo correctamente 
    Fecha y hora {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    Fin Status ETL
    """
    mensaje.attach(MIMEText(texto, 'plain'))

    # Conectarse al servidor SMTP y enviar el correo
    try:
        with smtplib.SMTP(smtp_server, puerto) as servidor:
            servidor.starttls()
            servidor.login(usuario, contraseña)
            servidor.sendmail(de, para, mensaje.as_string())
            #print('Correo electrónico enviado correctamente.')
            insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(),'Correo electrónico enviado correctamente.')
    except Exception as e:
        print(f'Error al enviar el correo electrónico: {e}')
        #insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(),f'Error al enviar el correo electrónico: {e}')
 
else: 
    # texto plano y/o HTML del cuerpo del mensaje
    texto = f"""Inicio Status ETL,
    Finalizo con error  
    Fecha y hora {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    Fin Status ETL
    """
    mensaje.attach(MIMEText(texto, 'plain'))

    # Conectarse al servidor SMTP y enviar el correo
    try:
        with smtplib.SMTP(smtp_server, puerto) as servidor:
            servidor.starttls()
            servidor.login(usuario, contraseña)
            servidor.sendmail(de, para, mensaje.as_string())
            #print('Correo electrónico enviado correctamente.')
         #   insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(),'Correo electrónico enviado correctamente.')
    except Exception as e:
        print(f'Error al enviar el correo electrónico: {e}')
        print('Fin prg con error')
        #insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(),f'Error al enviar el correo electrónico: {e}')

#insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(),'Fin ETL')
print('Fin prg ')        
#print("Datos insertados correctamente.")
#insertar_logs(cursor, conn, datetime.now(), fecha_alta_proceso, datetime.now(),"Datos insertados correctamente.")


#analisis de datos 
# Consulta SQL para obtener los datos
sql_query = "SELECT * FROM pronostico_clima;"
#lectura
df = pd.read_sql_query(sql_query, conn)

type(df)
df.shape
df.head()
df.tail()
df.info()
print (df.isnull().sum())
print (df.notnull().sum())
df.isna().sum()
df.describe(include='all')
df.describe().T

# Cerrar la conexión
cursor.close()
conn.close()

