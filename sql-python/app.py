import mysql.connector as mysql
import os
import pymongo
import json
from datetime import datetime
from operator import itemgetter
from flask import Flask, request, jsonify
from dotenv import load_dotenv, find_dotenv
from google.cloud import pubsub_v1
from google.auth import jwt
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

# MONGO DB

CLOUD_MONGO_DB = os.environ.get('CLOUD_MONGO_DB')

print(CLOUD_MONGO_DB)

# FLASK CONFIG
app = Flask(__name__)
app.debug = True


# GLOBAL VARIABLES

cursor = None
db = None
cloud = None
client = None
container = None
myMongoClient = None
myMongoDb = None
myMongoCollection = None

project_id = "sopes-326122"
topic_id = "notificacion-carga"
#publisher = pubsub_v1.PublisherClient()
#topic_path = publisher.topic_path(project_id, topic_id)

cantidad_sql = 0
cantidad_mongo = 0
tiempo = datetime.now()


def parse_date(date):
    current = date.split('/')
    # YY -- MM -- DD
    return current[2]+'/'+current[1]+'/'+current[0]


@app.route('/')
def home():
    return jsonify({"message": "api montada correctamente"})


@app.route('/iniciarCarga', methods=['GET'])
def init():
    global cloud
    global client
    global db
    global cursor
    global container
    global myMongoClient
    global myMongoDb
    global myMongoCollection
    try:
        print(CLOUD_SQL_HOST)
        print(CLOUD_SQL_USER)
        print(CLOUD_SQL_PASS)
        # CONEXION MYSQL
        db = mysql.connect(host=CLOUD_SQL_HOST, user=CLOUD_SQL_USER, password=CLOUD_SQL_PASS, database=CLOUD_SQL_DATA)
        cursor = db.cursor()
        # CONEXION AZURE MONGO DB
        myMongoClient = pymongo.MongoClient(CLOUD_MONGO_DB)
        print(myMongoClient)
        myMongoDb = myMongoClient['Tweets']
        myMongoCollection = myMongoDb['Tweet']
        return jsonify({"message": "Conexion creada con exito"})
    except Exception as e:
        print(e)
        return jsonify({"message": "Error al intenar conectar a db"}), 400


@app.route('/publicar', methods=['POST'])
def send():
    global cantidad_sql
    global cantidad_mongo
    status = 200
    dbInsertar = ""

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
        cantidad_sql += 1
    except Exception as e:
        status = 400
        dbInsertar = "SQL"
        print(e)
    try:

        myMongoCollection.insert_one(tweetData)
        cantidad_mongo += 1
    except Exception as e:
        status = 400
        dbInsertar = "Cosmo"
        print(e)

    if status == 400:
        return jsonify({"message": f'error to insert en { dbInsertar } db'}), 400

    return jsonify({"message": 'Tweet guardado con exito'}), 200


@app.route('/finalizarCarga', methods=['GET'])
def close():
    global tiempo
    time = datetime.strptime(parse_tiempo(datetime.now()),'%H:%M:%S')-datetime.strptime(parse_tiempo(tiempo),'%H:%M:%S')
    global db
    try:
        db.disconnect()
    except Exception as e:
        pass

    service_account_info = json.load(open("sopes-326122-3cdb97cea07b.json"))
    audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"
    credentials = jwt.Credentials.from_service_account_info(
        service_account_info, audience=audience
    )
    publisher_audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
    credentials_pub = credentials.with_claims(audience=publisher_audience)    
    publisher = pubsub_v1.PublisherClient(credentials=credentials_pub)
    topic_path = publisher.topic_path(project_id, topic_id)
    data = {
        "api":"python",
        "cantidad": cantidad_sql + cantidad_mongo,
        "tiempoDeCarga": str(time)
    }
    datas = json.dumps(data)
    datas = datas.encode("utf-8")
    future = publisher.publish(
        topic_path, datas, origin="api-python", username = "data"
    )
    return jsonify({"message": "connection closed"})


def parse_tiempo(time):
    return str(time.hour) + ":" + str(time.minute) + ":" + str(time.second)


if __name__ == '__main__':
    app.run()

