import os
import configparser
from flask_restful import Resource, reqparse
from flask import request, render_template, Response, jsonify
from pymongo import MongoClient

parser = reqparse.RequestParser()
parser.add_argument('option')


class Entry(Resource):

    def get(self):
        return Response(render_template('index.html',mimetype='text/html'))


class Rent(Resource):
    
    def mongo_init(self):
        curpath = os.path.dirname(os.path.abspath('__file__')) 
        cfgpath = os.path.join(curpath,'conf.ini')
        conf = configparser.RawConfigParser()
        conf.read(cfgpath, encoding="utf-8")
        username = conf.get('mongodb','dbuser')
        password = conf.get('mongodb','dbpasswd')
        monip = conf.get('mongodb','dbip')
        mondb = conf.get('mongodb','dbname')
        moncol= conf.get('mongodb','col')
        monclient = MongoClient(monip,
            username = username,
            password = password,
            authSource = mondb,
            authMechanism = 'SCRAM-SHA-256')
        mondb = monclient[mondb]
        monCol = mondb[moncol]
        return monCol

    def post(self):
        arg = parser.parse_args()
        monCol = self.mongo_init()

        if arg['option'] == "1":
            results = monCol.find({"$and":[{"縣市":"新北市"},{"性別要求":{"$in":["男女生皆可","男生"]}}]},{"_id":0})
        elif arg['option'] == "2":
            phone = request.form.get('phone')
            results = monCol.find({"聯絡電話":phone},{"_id":0})
        elif arg['option'] == "3":
            results = monCol.find({"出租者身份":{"$nin":["屋主"]}},{"_id":0})
        elif arg['option'] == "4":
            results = monCol.find({"$and":[{"縣市":"台北市"},{"出租者身份":{"$in":["屋主"]}},{"出租者":{"$regex":"小姐"}},{"出租者":{"$regex":"^吳"}}]},{"_id":0})

        return jsonify(list(results))