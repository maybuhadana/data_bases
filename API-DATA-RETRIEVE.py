#!/usr/bin/python3
# -*- coding: utf-8 -*-
import mysql.connector
import urllib.request as urllib2
import json
import mysql.connector
import numpy
import csv
import pandas as pd

API_KEY="deeb3ac8b7da11f7f0010c6aaeafaaa8"
API_URL="https://api.themoviedb.org/3/movie/"
SERVER_NAME = "mysqlsrv1.cs.tau.ac.il"
USER = "DbMysql56"
PASSWORD = "DbMysql56"
DB_NAME = "DbMysql56"
HOST = "localhost"
PORT ="3305"

def refactor_string(s):
    x = s.replace('"', '')
    x = x.replace("'", "")
    x = x.replace("(","")
    x = x.replace(")", "")
    return x

def get_data(id):
    url = API_URL + id + "?api_key=" + API_KEY
    response = urllib2.urlopen(url)
    data = json.load(response)
    dic = {}
    dic["movie_id"]= data["id"]
    dic["title"] = refactor_string(data["title"])
    dic["description"] = refactor_string(data["overview"])
    dic["language"] = refactor_string(data["original_language"])
    collecion = data["belongs_to_collection"]
    if(collecion):
        dic["collection_id"] = collecion["id"]
        dic["collection_name"] = refactor_string(collecion["name"])
    else:
        dic["collection_id"] = None
        dic["collection_name"] = None
    dic["adult"] = 1 if data["adult"]=="true" else 0
    dic["length"] = data["runtime"]
    dic["productions"] = [(company["id"], refactor_string(company["name"])) for company in data["production_companies"]]
    dic["budget"] = data["budget"]
    dic["revenue"] = data["revenue"]
    dic["genres"] = [refactor_string(genre["name"]) for genre in data["genres"]]
    return dic


def add_to_collection(cur,db,collection_id,collection_name):
    if (collection_id!=None):
        query = f'''INSERT IGNORE INTO collection(col_id, name) 
        VALUES ({collection_id},'{collection_name}');'''
        try:
            cur.execute(query)
            db.commit()
        except Exception as e:
            raise e
            db.rollback()

def add_to_movies(cur,db,movie_id, title, description, language, collection_id, adult, length):
    if (collection_id==None):
        query = f'''INSERT INTO movies(movie_id, title, description, language, adult, length) 
        VALUES ({movie_id},'{title}',"{description}",'{language}',{adult},{length});'''
    else:
        query = f'''INSERT INTO movies(movie_id, title, description, language, collection_id, adult, length) 
        VALUES ({movie_id},'{title}',"{description}",'{language}',{collection_id},{adult},{length});'''
    try:
        cur.execute(query)
        db.commit()
    except Exception as e:
        print(e)
        db.rollback()

def add_to_production(cur,db,productions):
    for prod in productions:
        production_id=prod[0]
        production_name=prod[1]
        query = f'''INSERT IGNORE INTO production (prod_id , name) 
        VALUES ({production_id},'{production_name}');'''
        try:
            cur.execute(query)
            db.commit()
        except Exception as e:
            raise e
            db.rollback()


def add_to_movie_production(cur,db,productions,movie_id):
    for prod in productions:
        production_id=prod[0]
        query = f'''INSERT INTO movie_production (production_id,movie_id) 
        VALUES ({production_id},{movie_id});'''
        try:
            cur.execute(query)
            db.commit()
        except Exception as e:
            raise e
            db.rollback()

def add_to_finance(cur,db,movie_id, budget, revenue):
    query = f'''INSERT INTO finance (movie_id ,budget ,revenue) 
    VALUES ({movie_id},{budget},{revenue});'''
    try:
        cur.execute(query)
        db.commit()
    except Exception as e:
        raise e
        db.rollback()

def add_to_genres(cur,db,genres,movie_id):
    for genre in genres:
        query = f'''INSERT INTO genres (movie_id,name) 
        VALUES ({movie_id},'{genre}');'''
        try:
            cur.execute(query)
            db.commit()
        except Exception as e:
            raise e
            db.rollback()

def add_to_raiting(cur,db):
    data = pd.read_csv("rating.csv")
    df = pd.DataFrame(data)
    for row in df.itertuples():
        query=f'''INSERT INTO rating (user_id , movie_id , rating)
        VALUES ({row.user_id},{row.movie_id},{row.rating});'''
        try:
            cur.execute(query)
            db.commit()
        except Exception as e:
            raise e
            db.rollback()

def fill_tables(dic,cur,cnx):
    add_to_collection(cur,cnx,dic["collection_id"],dic["collection_name"])
    add_to_movies(cur, cnx, dic["movie_id"], dic["title"], dic["description"], dic["language"], dic["collection_id"], dic["adult"], dic["length"])
    add_to_production(cur,cnx,dic["productions"])
    add_to_movie_production(cur,cnx,dic["productions"],dic["movie_id"])
    add_to_finance(cur,cnx,dic["movie_id"], dic["budget"], dic["revenue"])
    add_to_genres(cur, cnx, dic["genres"], dic["movie_id"])

ids=open('ids.csv', 'r').read().splitlines()
cnx = mysql.connector.connect(user=USER, password=PASSWORD, host=HOST, database=DB_NAME, port=PORT)
cur = cnx.cursor()
for movie_id in ids:
    try:
        dic=get_data(movie_id)
        fill_tables(dic, cur, cnx)
    except Exception as e:
        print (e)
        print(movie_id)
add_to_raiting(cur, cnx)
cnx.close()


