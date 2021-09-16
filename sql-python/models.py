from flask import Flask, request, jsonify, Response
from flask_sqlalchemy import SQLAlchemy

# FLASK CONFIG
app = Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:123@localhost/tweets'
#app.debug = True
db = SQLAlchemy(app)


class Tweet(db.Model):
    __tablename__ = "tweet"
    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(100))
    comentario = db.Column(db.String())
    fecha = db.Column(db.DateTime)

    def __init__(self, n, c, f):
        self.nombre = n
        self.comentario = c,
        self.fecha = f


class Hashtag(db.Model):
    __tablename__ = "hashtag"
    id = db.Column(db.Integer, primary_key=True)
    texto = db.Column(db.String(100))

    def __init__(self, n):
        self.texto = n


class TweetHashtag(db.Model):
    __tablename__ = "tweethashtag"
    id = db.Column(db.Integer, primary_key=True)
    upvotes = db.Column(db.Integer)
    downvotes = db.Column(db.Integer)
    tweet = db.Column(db.Integer, db.ForeignKey('tweet.id'))
    hashtag = db.Column(db.Integer, db.ForeignKey('hashtag.id'))

    def __init__(self, u, d, t, h):
        self.upvotes = u
        self.downvotes = d
        self.tweet = t
        self.hashtag = h
