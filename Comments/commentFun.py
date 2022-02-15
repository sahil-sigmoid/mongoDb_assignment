from datetime import datetime

from Comments.loadCommentsData import commentsData
from bson import ObjectId

def insert(collections, name, email, movie_id, text, date):
    collections.insert_one({"name":name,"email":email,"movie_id": ObjectId(movie_id),"text":text,"date":date})
    print("added successfully")


def maxCommentsbyUser(collections):
    dict={}
    for row in collections.find():
        email = row['email']
        if dict.get(email):
            dict[email]+=1
        else:
            dict[email]=1
    a = sorted(dict.items(), key=lambda x: x[1],reverse=True)
    return a[0:10]


def topMoviesWithMaxComment(collections,db):
    dic = {}
    for row in collections.find():
        movie_id = row['movie_id']
        if dic.get(movie_id):
            dic[movie_id] += 1
        else:
            dic[movie_id] = 1
    a = dict(sorted(dic.items(), key=lambda x: x[1], reverse=True))
    data = []
    c=0
    for k,v in a.items():
        x=db['movies'].find_one({"_id":ObjectId(k)})
        data.append(x['title'])
        c +=1
        if(c==10):
            break

    return data


def total_number_of_comment_in_year(collections,given_year):
    dic= {  "01":0,"02":0,  "03": 0, "04": 0,"05": 0,"06": 0,"07": 0,"08": 0, "09": 0,"10": 0,"11": 0,"12": 0
    }
    for i in collections.find():
        dte = int(i['date'])
        datetime_obj = datetime.fromtimestamp(dte / 1e3)
        date = datetime_obj.date()
        x = str(date)
        yr = x[0:4]
        mo = x[5:7]
        if(yr==given_year):
            dic[mo] +=1
    return dic

def Comments(db):

    # for loading data into comments collection
    commentsData(db)
    collections = db['comments']

    #inserting new comment into database collection (comments)
    # insert(collections,name ="sahil" , email="sahil@gmail.com", movie_id="573a13eff29313caabdd82f3", text="Awesome", date="1534253100622")

    # print top 10 users who made maximum comment
    top_users = maxCommentsbyUser(collections)
    print(" Top 10 users who made maximum comments")
    print(top_users)

    # print top 10 movies with maximum comments
    top_movies = topMoviesWithMaxComment(collections,db)
    print("Top 10 movies with maximum comments")
    print(top_movies)


    # all comments with given year
    comments_with_given_year = total_number_of_comment_in_year(collections,"2001")
    print("All comments with given year eg: 2001")
    print(comments_with_given_year)



