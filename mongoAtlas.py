# Python script to connect to a MongoDB Atlas cluster and upload data to it

import pymongo
import csv


def connect():

    username = input("Username: ")
    password = input("Password")

    connection_url = "mongodb+srv://" + username + ":" + password + "@hrs-cluster.gfzro.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    client = pymongo.MongoClient(connection_url)

    # Test for successful connection
    # print(client.test)
    return client


def loadToMongo():

    client = connect()

    # Navigating within the database
    database = client.hsl_database
    reviews = database.reviews
    new_reviews = []

    # Read data from a csv file and upload it to the cluster
    filename = "Review_db.csv"
    with open(filename, 'r') as csvfile:

        csv_reader = csv.reader(csvfile)
        fields = next(csv_reader)
        for row in csv_reader:

            record = {}
            for i in range(len(fields)):
                record[fields[i]] = row[i]

            new_reviews.append(record)

    # Bulk insert the records 
    result = reviews.insert_many(new_reviews)
    print(result)


# loadToMongo()
.