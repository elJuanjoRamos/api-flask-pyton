import mysql.connector as mysql
import azure.cosmos.cosmos_client as cosmos_client
import os
from operator import itemgetter
from flask import Flask, request, jsonify
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ENV VARIABLES SQL
CLOUD_SQL_HOST = os.environ.get("CLOUD_SQL_HOST")
CLOUD_SQL_DATA = os.environ.get('CLOUD_SQL_DATABASE_NAME')
CLOUD_SQL_PASS = os.environ.get('CLOUD_SQL_PASSWORD')
CLOUD_SQL_USER = os.environ.get('CLOUD_SQL_USERNAME')

# ENV VARIABLES AZURE
ACCOUNT_HOST = os.environ.get("ACCOUNT_HOST")
ACCOUNT_KEY = os.environ.get("ACCOUNT_KEY")
COSMOS_DATABASE = os.environ.get("COSMOS_DATABASE")
COSMOS_CONTAINER = os.environ.get("COSMOS_CONTAINER")

# FLASK CONFIG
app = Flask(__name__)
app.debug = True


# GLOBAL VARIABLES

cursor = None
db = None
cloud = None
client = None
container = None


def parse_date(date):
    current = date.split('/')
    # YY -- MM -- DD
    return current[2]+'/'+current[1]+'/'+current[0]


@app.route('/', methods=['GET'])
def home():
    return jsonify({"message": "api montada correctamente"})

@app.route('/iniciaCarga', methods=['GET'])
def init():
    global cloud
    global client
    global db
    global cursor
    global container
    try:
        #CONEXION MYSQL
        db = mysql.connect(host=CLOUD_SQL_HOST, user=CLOUD_SQL_USER, password=CLOUD_SQL_PASS, database=CLOUD_SQL_DATA)
        cursor = db.cursor()

        client = cosmos_client.CosmosClient(ACCOUNT_HOST, {'masterKey': ACCOUNT_KEY},
                                            user_agent="CosmosDBPythonQuickstart", user_agent_overwrite=True)
        cosmo_db = client.get_database_client(COSMOS_DATABASE)
        container = cosmo_db.get_container_client(COSMOS_CONTAINER)

        return jsonify({"message": "Conexion creada con exito"})
    except Exception as e:
        print(e)
        return jsonify({"message": "Error al intenar conectar a db"}), 400


@app.route('/publicar', methods=['POST'])
def send():

    status = 400
    tweetData = request.get_json()
    nombre, comentario, fecha, upvotes,  downvotes, \
    hashtags = itemgetter('nombre', 'comentario', 'fecha' , 'upvotes', 'downvotes', 'hashtags')(tweetData)

    list = ""
    for index, i in enumerate(hashtags):
        if index == 0:
            list = str(i)
        else:
            list = list + ',' + str(i)

    try:
        query = 'INSERT INTO Tweet(nombre, comentario, fecha, upvotes, downvotes, hashtags) VALUES(%s,%s,%s,%s,%s,%s)'
        cursor.execute(query, (nombre, comentario, parse_date(fecha), upvotes, downvotes, list))
        db.commit()

        tweetData['id'] = str(cursor.lastrowid)
        tweetData['pk'] = str(cursor.lastrowid)

        status = 200
    except Exception as e:
        print(e)
        return jsonify({"message": "error to insert en db google"}), 400
    if status == 200:
        try:
            print('\nCreating Items\n')
            container.create_item(body=tweetData)
            return jsonify({"message": "tweet saved"})
        except Exception as e:
            return jsonify({"message": "error to insert en cosmo db"}), 400


@app.route('/finalizarCarga', methods=['GET'])
def close():
    global db
    db.disconnect()
    return jsonify({"message": "connection closed"})


if __name__ == '__main__':
    app.run()

