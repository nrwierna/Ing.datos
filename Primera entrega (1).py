# Primera entrega
import requests  #realiza solicitudes HTTP - hacer una solicitud GET a una API pública y obtener los datos en formato JSON.
import psycopg2  # interactuar con bases de datos PostgreSQL. Conectarse a una base de datos Amazon Redshift (que está basada en PostgreSQL) 
                 #y realizar operaciones de base de datos como crear tablas e insertar datos.

# Configuración de la API
api_url = "https://jsonplaceholder.typicode.com/users"   #"URL_DE_LA_API"


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

# Consulta la API para tomar datos
response = requests.get(api_url)
usuarios_data = response.json()

# Crear tabla en Redshift 
create_table_query = """
CREATE TABLE IF NOT EXISTS usuarios (
    nombre VARCHAR,
    correo VARCHAR
);
"""

cursor.execute(create_table_query)
conn.commit()

# Insertar datos en la tabla

for usuario in usuarios_data:
    name = usuario["name"]
    email = usuario["email"]  # Agregar la clave "email" si está presente en el JSON
    insert_query = "INSERT INTO usuarios (nombre, correo) VALUES (%s, %s);"
    cursor.execute(insert_query, (name, email))

conn.commit()
print("Datos insertados correctamente.")



# Realizar la conexión
try:    
    cursor = conn.cursor()  

    # Ejecutar la consulta SELECT
    query = "SELECT * FROM usuarios"

    cursor.execute(query)

    # Obtener los resultados y mostrarlos en la consola
    results = cursor.fetchall()

    for row in results:
        print(row)


except (Exception, psycopg2.Error) as error:
    print("Error al conectar o ejecutar la consulta:", error)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("Conexión cerrada")
