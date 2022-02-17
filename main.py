
import pymongo
from Comments.commentFun import Comments
from Movies.moviesFun import Movies
from Theaters.theatersFun import Theaters
from Users.usersFun import Users
if __name__ == "__main__":
    # connecting to database
    client =pymongo.MongoClient('mongodb://localhost:27017/')
    #creating database
    db = client["mflix"]

    # print(" ******    Comments  **************")
    # Comments(db)
    # print()
    # print(" ******    Movies  **************")
    # Movies(db)
    # print()
    print(" ******    Theatres  **************")
    Theaters(db)

    # Users(db)








