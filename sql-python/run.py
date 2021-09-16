import models
from models import *
import azure.cosmos.cosmos_client as cosmos_client
import config
import os
import pymysql

#AZURE DB
HOST = config.settings['host']
MASTER_KEY = config.settings['master_key']
DATABASE_ID = config.settings['database_id']
CONTAINER_ID = config.settings['container_id']



db_connection_name = config.settings['CLOUD_SQL_CONNECTION_NAME']
db_user = config.settings['CLOUD_SQL_USERNAME']
db_password = config.settings['CLOUD_SQL_PASSWORD']
db_name = config.settings['CLOUD_SQL_DATABASE_NAME']



app = models.app
app.debug = True


def parse_date(date):
    try:
        date_new = date.split('/')
        return '20' + date_new[2] + '-' + date_new[1] + '-' + date_new[0]
    except:
        return '2021-01-01'



@app.route('/publicar', methods=['POST'])
def send():
    tweetData = request.get_json()
    #tweet = saveDb(Tweet(n=tweetData['nombre'], c=tweetData['comentario'], f=parse_date(tweetData['fecha'])))
    """
    if tweetData['hashtags']:
        list_hash = tweetData['hashtags']
        for i in list_hash:
            hash = Hashtag.query.filter_by(texto=i).first()
            if not hash:
                hash = saveDb(Hashtag(n=i))
            th = saveDb(TweetHashtag(u=tweetData['upvotes'],d=tweetData['downvotes'],t=tweet.id, h=hash.id))

    tweetData['id'] = tweet.id
    """

    create_items(tweetData)
    #add_tweet(tweetData['nombre'], tweetData['comentario'], parse_date(tweetData['fecha']))
    return Response(status=200)



def saveDb(element):
    db.session.add(element)
    db.session.commit()
    return element;


def create_items(item):
    print('\nCreating Items\n')

    order1 = {
        'id' :          str(item['id']),
        'nombre' :      item['nombre'],
        'pk':           str(item['id']),
        'comentario' :  item['comentario'],
        'fecha' :       item['fecha'],
        'upvotes' :     item['upvotes'],
        'downvotes' :   item['downvotes'],
        'hastags' :     item['hashtags']
    }
    container.create_item(body=order1)


def open_conetion():
    unix_socket = '/cloudsql/{}'.format(db_connection_name)

    try:
        if os.environ.get('GAE_ENV') == 'standard':
            conn = pymysql.connect(user=db_user, password=db_password,
                                unix_socket=unix_socket, db=db_name,
                            cursorclass=pymysql.cursors.DictCursor)
            return conn
    except pymysql.MySQLError as e:
        print(e)
        return None
    return None

def add_tweet(nombre, comentario, fecha):
    conn = open_conetion()
    if conn:
        with conn.cursor() as cursor:
            cursor.execute('INSERT INTO Tweet (nombre, comentario, fecha) VALUES(%s, %s, %s)', (nombre, comentario, fecha))
        conn.commit()
        conn.close()


if __name__ == '__main__':
    client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY}, user_agent="CosmosDBPythonQuickstart",
                                        user_agent_overwrite=True)

    db1 = client.get_database_client(DATABASE_ID)
    container = db1.get_container_client(CONTAINER_ID)

    #db.create_all()
    app.run()


