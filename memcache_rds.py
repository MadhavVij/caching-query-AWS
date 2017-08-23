import random

import pymysql
from flask import Flask, render_template, request
import memcache
import os
import hashlib
import time

app = Flask(__name__)
memC = memcache.Client([''], debug=0)


def connectDB():
    return pymysql.connect(host='', port=3306, user='',
                           password='', db='', local_infile=True)


def createDB():
    conn = connectDB()
    cur = conn.cursor()
    cur.execute("""DROP TABLE IF EXISTS data""")
    conn.commit()
    query = """
    CREATE TABLE data (
        `Gender` VARCHAR(6) CHARACTER SET utf8,
        `GivenName` VARCHAR(9) CHARACTER SET utf8,
        `Surname` VARCHAR(8) CHARACTER SET utf8,
        `StreetAddress` VARCHAR(19) CHARACTER SET utf8,
        `City` VARCHAR(11) CHARACTER SET utf8,
        `State` VARCHAR(2) CHARACTER SET utf8,
        `EmailAddress` VARCHAR(33) CHARACTER SET utf8,
        `Username` VARCHAR(9) CHARACTER SET utf8,
        `TelephoneNumber` VARCHAR(12) CHARACTER SET utf8,
        `Age` INT,
        `BloodType` VARCHAR(2) CHARACTER SET utf8,
        `Centimeters` INT,
        `Latitude` NUMERIC(8, 6),
        `Longitude` NUMERIC(8, 6)
    );"""

    cur.execute(query)
    conn.commit()

    path = app.root_path + r'\input\boat.csv'

    query = """ LOAD DATA LOCAL INFILE 'FILE_PATH' INTO TABLE
              data FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED
              BY '"' Lines terminated by '\n' IGNORE 1 LINES; """

    # ' LOAD DATA LOCAL INFILE "'+path+'" INTO TABLE boat1 FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY ' \
    #                                     '\'"\' ESCAPED BY \'"\' Lines terminated by "\n" IGNORE 1 LINES'
    print(query)
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()



def generateQuery(r1,r2,choice):

    lowerHeight = random.randrange(int(r1), int(r2)-1)
    upperHeight = random.randrange(lowerHeight + 1, int(r2)+1)
    query = 'select GivenName,City, State, Centimeters from data WHERE state like "%'+state+'" and Centimeters BETWEEN "' + str(lowerHeight) + '" ' \
                                                                                                                'and "' + \
                str(upperHeight) + '"'

    if choice == 'mem':
        data = fromMemcache(query)

    elif choice == 'db':
        data = fromDB(query)

    else:
        return 'Incorrect Input'
    return data


def fromDB(sql):
    conn = connectDB()
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return data


def fromMemcache(sql):
    conn = connectDB()
    cur = conn.cursor()

    hash = hashlib.sha256(sql).hexdigest()
    # print(hash)
    key = 'cache:' + hash
    # print("Key= ")
    # print(key)

    if memC.get(key):
        # print("used memcache")
        return memC.get(key)

    else:
        # print("add to memcache")
        cur.execute(sql)
        data = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        memC.set(key, data, time=500)

        return memC.get(key)

@app.route('/process',methods=['POST','GET'])
def process():
    start_time = time.time()
    result = []
    if request.method == 'POST':
        r1 = request.form['range1']
        r2 = request.form['range2']
        choice = request.form['type']
        times = request.form['times']

        for i in range(1, int(times)+1):
            result.append(generateQuery(r1,r2,choice))

    end_time = time.time()
    print('time taken: ')
    time_taken = end_time - start_time
    print(time_taken)
    result.append(time_taken)
    return render_template('display.html', result=result)





@app.route('/')
def hello_world():
    conn = connectDB()
    cur = conn.cursor()
    query = 'select COUNT(*) from data'
    cur.execute(query)
    result = cur.fetchone()
    count = result[0]
    conn.commit()
    cur.close()
    conn.close()
    return render_template('index.html', count=count)

    # start_time = time.time()
    # for i in range(1, 5001):
    #     generateQuery()
    # end_time = time.time()
    # print('time taken: ')
    # print(end_time - start_time)
    # # start_time = time.time()
    # # memcache()
    # # end_time = time.time()
    # # print('time taken: ')
    # # print(end_time-start_time)
    # # #createDB()
    # return 'Hello World!'


port = os.getenv('PORT', '80')
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(port))


################################################################################################

@app.route('/processData',methods=['POST','GET'])
def processData():
    start_time = time.time()
    result = []
    if request.method == 'POST':
        lname = request.form['surname']
        choice = request.form['type']
        query = 'select GivenName,Surname,TelephoneNumber,State from data WHERE Surname like "%'+lname+'"'
        if choice == 'mem':
            data = fromMemcache(query)
            for row in data:
                tuple = (row[0], row[1], row[2], row[3])
                result.append(tuple)

        elif choice == 'db':
            data = fromDB(query)
            for row in data:
                tuple = (row[0], row[1], row[2], row[3])
                result.append(tuple)

        else:
            return 'Incorrect Input'

    end_time = time.time()
    print('time taken: ')
    time_taken = end_time - start_time
    print(time_taken)
    result.append(time_taken)
    return render_template('display.html', result=result)

@app.route('/processData2',methods=['POST','GET'])
def processData2():
    start_time = time.time()
    result = []
    if request.method == 'POST':
        state = request.form['state']
        r1 = request.form['range1']
        r2 = request.form['range2']
        choice = request.form['type']
        count = 'select count(*) from data WHERE state like "%'+state+'" and Centimeters BETWEEN "' + str(r1) + '" ' \
                                                                                                                'and "' + \
                str(r2) + '"'

        query = 'select GivenName,City, State, Centimeters from data WHERE state like "%'+state+'" and Centimeters BETWEEN "' + str(r1) + '" ' \
                                                                                                                'and "' + \
                str(r2) + '"'
        if choice == 'mem':
            count = fromMemcache(count)
            result.append(count)
            data = fromMemcache(query)
            for row in data:
                tuple = (row[0], row[1], row[2], row[3])
                result.append(tuple)

        elif choice == 'db':
            count = fromDB(count)
            result.append(count)
            data = fromDB(query)
            for row in data:
                tuple = (row[0], row[1], row[2], row[3])
                result.append(tuple)

        else:
            return 'Incorrect Input'

    end_time = time.time()
    print('time taken: ')
    time_taken = end_time - start_time
    print(time_taken)
    result.append(time_taken)
    return render_template('display.html', result2=result)




@app.route('/processData4',methods=['POST','GET'])
def processData4():
    start_time = time.time()
    result = []
    if request.method == 'POST':
        state = request.form['state']
        r1 = request.form['range1']
        r2 = request.form['range2']
        choice = request.form['type']
        query = 'select Gender, Age from DATA WHERE state like "%' + \
                state + '" and ' \
                        'Centimeters BETWEEN ' \
                        '"' + str(
            r1) + '" ' \
                  'and "' + \
                str(r2) + '"'
        if choice == 'mem':
            data = fromMemcache(query)
            for row in data:
                if row[0] == 'female':
                    age = int(row[1]) - 1
                    query = 'update data set Age = "'+age+'" where state like "%' + \
                         state + '" and ' \
                        'Centimeters BETWEEN "' + str(
                         r1) + '" ' \
                        'and "' + \
                        str(r2) + '"'
                    fromMemcache(query)

                elif row[0] == 'male':
                    age = int(row[1]) + 1
                    query = 'update data set Age = "' + age + '" where state like "%' + \
                            state + '" and ' \
                                    'Centimeters BETWEEN "' + str(
                        r1) + '" ' \
                              'and "' + \
                            str(r2) + '"'
                    fromMemcache(query)





        elif choice == 'db':

            data = fromDB(query)
            for row in data:
                if row[0] == 'female':
                    age = int(row[1]) - 1
                    query = 'update data set Age = "' + age + '" where state like "%' + \
                            state + '" and ' \
                                    'Centimeters BETWEEN "' + str(
                        r1) + '" ' \
                              'and "' + \
                            str(r2) + '"'
                    fromDB(query)

                elif row[0] == 'male':
                    age = int(row[1]) + 1
                    query = 'update data set Age = "' + age + '" where state like "%' + \
                            state + '" and ' \
                                    'Centimeters BETWEEN "' + str(
                        r1) + '" ' \
                              'and "' + \
                            str(r2) + '"'
                    fromDB(query)


        else:
            return 'Incorrect Input'

    return 'Updated!'