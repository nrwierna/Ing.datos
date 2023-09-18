
# Segunda entrega
import requests  #realiza solicitudes HTTP - hacer una solicitud GET a una API pública y obtener los datos en formato JSON.
import psycopg2  # interactuar con bases de datos PostgreSQL. Conectarse a una base de datos Amazon Redshift (que está basada en PostgreSQL) 
                 #y realizar operaciones de base de datos como crear tablas e insertar datos.
from datetime import datetime, timedelta

# hora inicio
#print(f"inicio script: {datetime.now()}")

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
            precipitacion       VARCHAR
 );
 """

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

# Verificar el resultado
if tabla_existe:
    print(f"La tabla '{nombre_tabla}' ya existe.")
else:
    cursor.execute(create_table_query)

print(f"Se creó la tabla '{nombre_tabla}'.")
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

# hora creacion tabla
print(f"creacion de tabla: {datetime.now()}")

# URL de la API de RapidAPI para obtener el pronóstico actual
api_url = 'https://weatherapi-com.p.rapidapi.com/history.json'

# Cabeceras requeridas para la API de RapidAPI
headers = {
	"X-RapidAPI-Key": "6ab9bce469msh4228a762e86ee73p1cd9e0jsn4449ff6aa519",
	"X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
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

# hora solicitud datos
print(f"solicitud de datos: {datetime.now()}")

# Itera a través de las ciudades en el diccionario
id_registro = 0
for city_name, city_coords in cities_data.items():
  #print("Aqui entra con" , city_name)
  for fecha in fechas:
    #for fecha in fechas:
    #print ("Aqui la fecha es", fecha)
    lat, lon = city_coords.split(',')
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
        id_registro = id_registro + 1
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
        if resultado is not None:
            print("El registro ya existe en la tabla.")
        else:
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
                precipitacion
                ) VALUES (%s,%s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
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
                precipitacion
            ))
         
        conn.commit()

    else:
        print(f'Error al conectarse a la API para la ciudad {city_name}. Código de estado:', response.status_code)

# hora fin solicitud de datos
print(f"fin solicitud datos: {datetime.now()}")

# Imprime los datos del clima por ciudad
for city_name, weather_data in cities_weather_data.items():
    print(f'Ciudad: {city_name}')
    for key, value in weather_data.items():
        print(f'{key}: {value}')
    print('-' * 40)

# hora fin impresion datos
print(f"fin impresion: {datetime.now()}")

# Cerrar la conexión
cursor.close()
conn.close()
print("Datos insertados correctamente.")


