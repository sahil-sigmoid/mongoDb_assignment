from Movies.loadMoviesData import moviesData


def maxImdbRating(collections):
    res = collections.aggregate([
        {"$group": {"_id": {"title": "$title", "imdb": "$imdb.rating"}}},
        {"$sort": {"_id.imdb": -1}},
        {"$limit": 1}
    ])
    for i in res:
        print(i)

def maxRatingWithGivenYear(collections,year):
    res = collections.aggregate([
        {"$project": {"year": {"$year": "$released"}, "rating": "$imdb.rating", "title": "$title"}},
        {"$match":{"year":year}},
        {'$group': { '_id': {"title":"$title","rating":"$rating"}}},
        {"$sort": {"_id.rating": -1}},
        {"$limit": 1}
    ])
    for i in res:
        print(i)


def maxImdbRatingWithMaximumVotes(collections):
    res = collections.aggregate([
        {"$project": {"votes": "$imdb.votes", "rating": "$imdb.rating", "title": "$title"}},
        {"$match": {"votes": {"$gt": 1000}}},
        {"$group": {"_id": {"title": "$title", "rating": "$rating", "votes": "$votes"}}},
        {"$sort": {"_id.rating": -1, "_id.votes": -1}},
        {"$limit": 1}
    ])
    for x in res:
        print(x)


def titleMatching(collections,patt):
    res = collections.aggregate([
        {"$match": {  "title":{"$regex": patt} } }
    ])

    for i in res:
        print(i)

def whoCreatedMaxMovie(collection):
    agg_result = collection.aggregate([
        {"$group":{"_id": "$directors","no_films": {"$sum": 1}}},
        {"$sort":{"no_films":-1}},
        {"$limit":1}
        ])
    for i in agg_result:
        print(i)

def whoCreatedMaxMovieGivenGenres(collections, genres):
    agg_result = collections.aggregate([
        {"$project": {"genres":"$genres", "rating": "$imdb.rating", "directors": "$directors"}},
        {"$match": {"genres": genres}},
        {"$group": {"_id": "$directors", "no_films": {"$sum": 1}}},
        {"$sort": {"no_films": -1}},
        {"$limit": 1}
    ])
    for i in agg_result:
        print(i)


def whoCreatedMaxMovieWithGivenYear(collections,year):
    agg_result = collections.aggregate([
        {"$project": {"year": {"$year": "$released"}, "rating": "$imdb.rating", "directors": "$directors"}},
        {"$match":{"year":year}},
        {"$group": {"_id": "$directors", "no_films": {"$sum": 1}}},
        {"$sort": {"no_films": -1}},
        {"$limit": 1}
    ])
    for i in agg_result:
        print(i)

def whoStarredMaxNumber(collections):
    agg_result = collections.aggregate(
        [{"$group":{"_id": "$cast", "no_films": {"$sum": 1} }},
         {"$sort": {"no_films": -1}},
         {"$limit": 10}
        ])
    for i in agg_result:
        print(i)

def whoStarredMaxNumberGivenYear(collections,year):
    agg_result = collections.aggregate([
        {"$project": {"year": {"$year": "$released"}, "casr": "$cast"}},
            {"$match":{"year":year}},
            {"$group": {"_id": "$cast", "no_films": {"$sum": 1}}},
         {"$sort": {"no_films": -1}},
         {"$limit": 1}
         ])
    for i in agg_result:
        print(i)

def whoStarredMaxNumberGivenGenres(collections,genres):
    agg_result = collections.aggregate(
        [{"$match": {"genres": genres}},
         {"$group": {"_id": "$cast","no_films": {"$sum": 1}}},
         {"$sort":{"no_films":-1}},
         {"$limit":1}
        ])
    for i in agg_result:
        print(i)


def moviesWithEachGenreWithHighestImdbRating(collections):
    agg_result = collections.aggregate(
        [{'$group':     #grouping acc to gene with first title come having maximum rating
        {
            '_id': "$genres",
             'max_rating': {'$max':'$imdb.rating'},
            'title': {'$first':'$title'}
        }},
            {"$limit": 10}

    ])
    for i in agg_result:
        print(i)




def Movies(db):
    #loading data into movies collection
    moviesData(db)

    collections = db['movies']
    for i in collections.find():
        print(i)
        break

     # the highest IMDB rating
    print("Max IMDB rating Movie")
    maxImdbRating(collections)

    # the highest IMDB rating with given year
    print("Max IMDB rating Movie with given year")
    maxRatingWithGivenYear(collections,2014)

    # with highest IMDB rating with number of votes > 1000
    print("With highest IMDB rating with number of votes > 1000")
    maxImdbRatingWithMaximumVotes(collections)

    # with title matching a given pattern sorted by highest tomatoes ratings
    print("Find all title matching with given pattern")
    titleMatching(collections,"Band of")

    # who created the maximum number  of movies
    print("who created the maximum number  of movies")
    whoCreatedMaxMovie(collections)

    # who created the maximum number  of movies in a given Year
    print("who created the maximum number  of movies in a given Year")
    whoCreatedMaxMovieWithGivenYear(collections,2006)

    # who created the maximum number of  movies for a given genre
    print("who created the maximum number  of movies  for a given genre")
    whoCreatedMaxMovieGivenGenres(collections,["Documentary","Short"])

    # who starred in the maximum number of movies
    print("who starred in the maximum number of movies")
    whoStarredMaxNumber(collections)

    # who starred in the maximum number of movies in a given Year
    print("who starred in the maximum number of movies in a given Year")
    whoStarredMaxNumberGivenGenres(collections,2004)

    print("Find top `N` movies for each genre with the highest IMDB rating")
    moviesWithEachGenreWithHighestImdbRating(collections)

