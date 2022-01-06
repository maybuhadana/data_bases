#!/usr/bin/python3
# -*- coding: utf-8 -*-
import mysql.connector

SERVER_NAME = "mysqlsrv1.cs.tau.ac.il"
USER = "DbMysql56"
PASSWORD = "DbMysql56"
DB_NAME = "DbMysql56"
HOST = "localhost"
PORT ="3305"


MOVIES = '''CREATE TABLE movies(
            movie_id int NOT NULL,
            title varchar(255) NOT NULL, 
            description text,
            language char(20) NOT NULL, 
            collection_id int,
            adult tinyint(1),
            length smallint(5) NOT NULL,
            PRIMARY KEY (movie_id),
            FOREIGN KEY (collection_id) REFERENCES collection(col_id)
            );
            '''

COLLECTION = '''CREATE TABLE collection(
            col_id int NOT NULL ,
            name varchar(255),
            PRIMARY KEY (col_id)
            );
            '''

PRODUCTION = '''CREATE TABLE production(
            prod_id int NOT NULL ,
            name varchar(50) NOT NULL,
            PRIMARY KEY (prod_id)
            );
            '''

MOVIE_PRODUCTION = '''CREATE TABLE movie_production(
            production_id int NOT NULL ,
            movie_id int NOT NULL,
            PRIMARY KEY (production_id,movie_id),
            FOREIGN KEY (production_id) REFERENCES production(prod_id),
            FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
            );
            '''

FINANCE = '''CREATE TABLE finance(
            movie_id int NOT NULL ,
            budget int UNSIGNED,
            revenue int UNSIGNED,
            PRIMARY KEY (movie_id),
            FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
            );
            '''

GENRES = '''CREATE TABLE genres(
            movie_id int NOT NULL ,
            name varchar(30),
            PRIMARY KEY (movie_id,name),
            FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
            );
            '''

RATING = '''CREATE TABLE rating(
            user_id int NOT NULL,
            movie_id int NOT NULL,
            rating tinyint(5),
            PRIMARY KEY (movie_id,user_id),
            FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
            );
            '''


def create_table(db, cursor, query):
    try:
        cursor.execute(query)
        db.commit()
    except():
        db.rollback()

cnx = mysql.connector.connect(user=USER, password=PASSWORD, host=HOST, database=DB_NAME, port=PORT)
cur = cnx.cursor()
tables = [COLLECTION, MOVIES, PRODUCTION, MOVIE_PRODUCTION, FINANCE, GENRES, RATING]
tables_str = ["collection", "movies", "production", "movie_production", "finance", "genres", "rating"]
for table in tables_str[::-1]:
   cur.execute(f"DROP TABLE {table}")
for table in tables:
   create_table(cnx, cur, table)
cnx.close()



