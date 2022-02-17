from datetime import datetime

from Comments.loadCommentsData import commentsData
from bson import ObjectId

def insert(collections, name, email, movie_id, text, date):
    collections.insert_one({"name":name,"email":email,"movie_id": ObjectId(movie_id),"text":text,"date":date})
    print("added successfully")


def maxCommentsbyUser(collections):
    res = collections.aggregate([
        {"$group": {"_id": "$name", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ])
    for i in res:
        print(i)


def topMoviesWithMaxComment(collections,db):
    res = collections.aggregate([
        {"$group": {"_id": "$movie_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ])
    for i in res:
        print(i)


def total_number_of_comment_in_year(collections,given_year):
    res = collections.aggregate([

        {"$project": {"month": {"$month": "$date"}, "year": {"$year": "$date"}}}, #convert date into month store in month variable and date into year store in year variable
        {"$match": {"year": given_year}},
        {"$group": {"_id": {"month": "$month"}, "count": {"$sum": 1}}},
        {"$sort": {"_id.month": 1}}
    ])
    for i in res:
        print(i)

def Comments(db):

    # for loading data into comments collection
    # commentsData(db)
    collections = db['comments']


    #inserting new comment into database collection (comments)
    # insert(collections,name ="sahil" , email="sahil@gmail.com", movie_id="573a13eff29313caabdd82f3", text="Awesome", date="1534253100622")

    for i in collections.find():
        print(i)
        break

   # print top 10 users who made maximum comment
    print(" Top 10 users who made maximum comments")
    maxCommentsbyUser(collections)



    # print top 10 movies with maximum comments

    print("Top 10 movies with maximum comments")
    topMoviesWithMaxComment(collections,db)


    # all comments with given year

    print("All comments with given year eg: 2012")
    total_number_of_comment_in_year(collections, 2012)


