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

# MONGO DB

CLOUD_MONGO_DB = os.environ.get('CLOUD_MONGO_DB')

# FLASK CONFIG
app = Flask(__name__)
app.debug = True


# GLOBAL VARIABLES
project_id = "sopes-326122"
topic_id = "notificacion-carga"
cantidad = 0
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
    global cantidad
    global tiempo
    cantidad = 0
    tiempo = datetime.now()

    return jsonify({"message": "Conexion creada con exito"})


@app.route('/publicar', methods=['POST'])
def send():
    global cantidad

    # CONEXION MYSQL
    db = mysql.connect(host=CLOUD_SQL_HOST, user=CLOUD_SQL_USER, password=CLOUD_SQL_PASS, database=CLOUD_SQL_DATA)
    cursor = db.cursor()
    # CONEXION AZURE MONGO DB
    myMongoClient = pymongo.MongoClient(CLOUD_MONGO_DB)
    myMongoDb = myMongoClient['Tweets']
    myMongoCollection = myMongoDb['Tweet']

    status = 200

    tweetData = request.get_json()
    nombre, comentario, fecha, upvotes,  downvotes, hashtags = \
        itemgetter('nombre', 'comentario', 'fecha' , 'upvotes', 'downvotes', 'hashtags')(tweetData)

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
    except Exception as e:
        status = 400
        print(e)
    try:

        myMongoCollection.insert_one(tweetData)
    except Exception as e:
        status = 400
        print(e)

    if status == 400:
        return jsonify({"message": f'error to insert en las bases de datos db'}), 400
    cantidad += 1
    return jsonify({"message": 'Tweet guardado con exito'}), 200


@app.route('/finalizarCarga', methods=['GET'])
def close():
    global tiempo
    global cantidad
    time = (datetime.now().minute - tiempo.minute)*60


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
        "cantidad": cantidad,
        "tiempoDeCarga": str(time)
    }
    datas = json.dumps(data)
    datas = datas.encode("utf-8")
    publisher.publish( topic_path, datas, origin="api-python", username = "data" )
    return jsonify({"message": "connection closed"})


if __name__ == '__main__':
    app.run()

