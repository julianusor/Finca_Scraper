from re import MULTILINE
import mysql.connector
import json
import time

mydb = mysql.connector.connect(
    host="localhost", user="root", password="", autocommit="True"
)

mycursor = mydb.cursor()
dbname = "fincaraiz_db"


def is_database():
    '''checks if database dbname exists'''
    mycursor.execute("SHOW DATABASES")
    exists = False
    for x in mycursor:
        if x[0] == dbname:
            exists = True
    return exists


def is_table(tablename, createtable):
    ''' checks if table tablename exists, second parameter to create table '''
    mycursor.execute(f"USE {dbname}")
    mycursor.execute("SHOW TABLES")

    istable = False
    for x in mycursor:
        if x[0] == tablename.lower():
            istable = True

    if not istable and createtable:

        mycursor.execute(
            """
            CREATE TABLE Category (
            CategoryId VARCHAR(1) PRIMARY KEY,
            Category VARCHAR(12)
            );
            """
        )

        mycursor.execute(
            """
            CREATE TABLE Department (
            DepartmentId VARCHAR(2) PRIMARY KEY,
            Department VARCHAR(20)
            );
            """
        )

        mycursor.execute(
            """
            CREATE TABLE City (
            CityId VARCHAR(7) PRIMARY KEY,
            City VARCHAR(20)
            );
            """
        )
        mycursor.execute(
            """
            CREATE TABLE Age (
            AgeId VARCHAR(2) PRIMARY KEY,
            Age VARCHAR(20)
            );
            """
        )

        mycursor.executemany(
            "INSERT INTO `Category` (`CategoryId`, `Category`) VALUES (%s, %s)",
            [("8", "Apartamento"), ("9", "Casa"), ("1", "Proyecto Nuevo")],
        )
        mycursor.executemany(
            "INSERT INTO `age` (`AgeId`, `Age`) VALUES (%s, %s)",
            [
                ("0", "No data"),
                ("1", "Menos de 1 año"),
                ("2", "1 a 8 años"),
                ("5", "Más de 30 años"),
                ("3", "9 a 15 años"),
                ("4", "16 a 30 años"),
            ],
        )
        mycursor.executemany(
            "INSERT INTO `department` (`DepartmentId`, `Department`) VALUES (%s, %s)",
            [("67", "Cundinamarca"), ("55", "Antioquia")],
        )

        mycursor.executemany(
            "INSERT INTO `city` (`CityId`, `City`) VALUES (%s, %s)",
            [
                ("5500006", "Medellín"),
                ("5500001", "Envigado"),
                ("5500016", "Sabaneta"),
                ("5500004", "Rionegro"),
                ("5500005", "Bello"),
                ("5500002", "Itaguí"),
                ("5500003", "La Estrella"),
                ("3630001", "Bogotá"),
                ("6700003", "Chia"),
                ("6700004", "Soacha"),
                ("6700016", "Fusagasugá"),
            ],
        )

        mycursor.execute(
            f"""CREATE TABLE {tablename} 
            (Id VARCHAR(10) PRIMARY KEY,
            CategoryId VARCHAR(1),
            DepartmentId VARCHAR(2),
            CityId VARCHAR(7) ,
            AgeId VARCHAR(2) , 

            INDEX(CategoryId, DepartmentId, CityId, AgeId),
            
            FOREIGN KEY (CategoryId) 
                REFERENCES Category(CategoryId),
            FOREIGN KEY (DepartmentId) 
                REFERENCES Department(DepartmentId),
            FOREIGN KEY (CityId) 
                REFERENCES City(CityId),
            FOREIGN KEY (AgeId) 
                REFERENCES Age(AgeId),


            Price BIGINT,
            Surface DOUBLE,
            Area DOUBLE,
            Rooms TINYINT,
            Baths TINYINT, 
            Stratum TINYINT, 
            Garages TINYINT, 
            Latitude DOUBLE,
            Longitude DOUBLE,
            
            DateAdded TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP)ENGINE=INNODB"""
        )

        mydb.commit()



def is_in_db(propid):
    "checks if id is in table"


    
    mycursor.execute(f"SELECT * FROM `property` WHERE id = '{propid}'")
    
    myresult = mycursor.fetchall()
    
    if len(myresult) < 1:
        return False

    else: 
        return True

def add_new(d):
    '''adds a new record'''


    sql = f"INSERT INTO `property` (`Id`, `CategoryId`, `DepartmentId`, `CityId`, `AgeId`, `Price`, `Surface`, `Area`, `Rooms`, `Baths`, `Stratum`, `Garages`, `Latitude`, `Longitude`) VALUES ('{d[0]}', '{d[1]}', '{d[2]}', '{d[3]}', '{d[4]}', '{d[5]}', '{d[6]}', '{d[7]}', '{d[8]}', '{d[9]}', '{d[10]}', '{d[11]}', '{d[12]}', '{d[13]}')"
    
    mycursor.execute(sql)
    mydb.commit()
    return True


def initiate():
    "starts the scraper"
    existsdb = is_database()
    if not existsdb:
        mycursor.execute(f"CREATE DATABASE {dbname}")

    is_table("Property", True)

    return True
