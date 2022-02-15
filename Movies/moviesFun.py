from Movies.loadMoviesData import moviesData


def maxImdbRating(collections):
    movie_name=""
    rating=0.0
    for i in collections.find():
        if( i.get('imdb')):
            if i['imdb'].get('rating'):
                x = float(i['imdb']['rating'])
                if x > rating:
                    movie_name=i['title']
                    rating=x
    return [movie_name,rating]

def maxRatingWithGivenYear(collections,year):
    movie_name = ""
    rating = 0.0
    for i in collections.find({'year':year}):

            if (i.get('imdb')):
                if i['imdb'].get('rating'):
                    x = float(i['imdb']['rating'])
                    if x > rating:
                        movie_name = i['title']
                        rating = x
    return [movie_name, rating]

def maxImdbRatingWithMaximumVotes(collections):
    movie_name = ""
    rating = 0.0
    vote=0
    for i in collections.find():
        if (i.get('imdb')):
            vt = i['imdb']['votes']
            if( vt!= ""):
                if int(vt)>1000 and i['imdb'].get('rating'):
                    vote=vt
                    x = float(i['imdb']['rating'])
                    if x > rating:
                        movie_name = i['title']
                        rating = x
    return [movie_name, rating,vote]

def titleMatching(collections,patt):
    query = {
        "title": {
            "$regex": patt,
            "$options": 'i'  # case-insensitive
        }
    }
    ans = []
    for i in collections.find(query):
        ans.append(i['title'])
    return ans

def whoCreatedMaxMovie(collection):
    agg_result = collection.aggregate(
        [{
            "$group":
                {"_id": "$directors",
                 "no_films": {"$sum": 1}
                 }}
        ])
    director = []
    films=0
    for i in agg_result:
        if i['no_films']>films and i['_id']!=None:
            # print(i)
            films=i['no_films']
            director = i['_id']

    return [director,films]

def whoCreatedMaxMovieGivenGenres(collections, genres):
    agg_result = collections.aggregate(
        [
            {
                "$match": {
                    "genres": genres
                }
            },
            {
                "$group":
                    {"_id": "$directors",
                     "no_films": {"$sum": 1}
                     }}
        ])
    director = []
    films = 0
    for i in agg_result:
        # print(i)
        if i['no_films'] > films and i['_id'] != None:
            # print(i)
            films = i['no_films']
            director = i['_id']

    return [director, films]


def whoCreatedMaxMovieWithGivenYear(collections,year):
    agg_result = collections.aggregate(
        [
            {
                "$match": {
                    "year": year
                }
            },
            {
            "$group":
                {"_id": "$directors",
                 "no_films": {"$sum": 1}
                 }}
        ])
    director = []
    films = 0
    for i in agg_result:
        # print(i)
        if i['no_films'] > films and i['_id'] != None:
            # print(i)
            films = i['no_films']
            director = i['_id']

    return [director, films]

def whoStarredMaxNumber(collections):
    agg_result = collections.aggregate(
        [{
            "$group":
                {"_id": "$cast",
                 "no_films": {"$sum": 1}
                 }}
        ])
    cast = []
    films = 0
    for i in agg_result:
        if i['no_films'] > films and i['_id'] != None:
            # print(i)
            films = i['no_films']
            cast = i['_id']

    return [cast, films]

def whoStarredMaxNumberGivenYear(collections,year):
    agg_result = collections.aggregate(
        [
            {
                "$match": {
                    "year": year
                }
            },

            {
            "$group":
                {"_id": "$cast",
                 "no_films": {"$sum": 1}
                 }}
        ])
    cast = []
    films = 0
    for i in agg_result:
        if i['no_films'] > films and i['_id'] != None:
            # print(i)
            films = i['no_films']
            cast = i['_id']

    return [cast, films]

def whoStarredMaxNumberGivenGenres(collections,genres):
    agg_result = collections.aggregate(
        [
            {
                "$match": {
                    "genres": genres
                }
            },

            {
            "$group":
                {"_id": "$cast",
                 "no_films": {"$sum": 1}
                 }}
        ])
    cast = []
    films = 0
    for i in agg_result:
        if i['no_films'] > films and i['_id'] != None:
            # print(i)
            films = i['no_films']
            cast = i['_id']

    return [cast, films]


def moviesWithEachGenreWithHighestImdbRating(collections):
    agg_result = collections.aggregate(
        [{
        '$group':     #grouping acc to gene with first title come having maximum rating
        {
            '_id': "$genres",
             'max_rating': {'$max':'$imdb.rating'},
            'title': {'$first':'$title'}
        }
        }
    ])
    ans = []
    for i in agg_result:
        print(i)




def Movies(db):
    #loading data into movies collection
    moviesData(db)

    collections = db['movies']

     # the highest IMDB rating
    print("Max IMDB rating Movie")
    print(maxImdbRating(collections))

    # the highest IMDB rating with given year
    print("Max IMDB rating Movie with given year")
    print(maxRatingWithGivenYear(collections,"2010"))

    # with highest IMDB rating with number of votes > 1000
    print("With highest IMDB rating with number of votes > 1000")
    print(maxImdbRatingWithMaximumVotes(collections))

    # with title matching a given pattern sorted by highest tomatoes ratings
    print("Find all title matching with given pattern")
    print(titleMatching(collections,"Band of"))

    # who created the maximum number  of movies
    print("who created the maximum number  of movies")
    print(whoCreatedMaxMovie(collections))

    # who created the maximum number  of movies in a given Year
    print("who created the maximum number  of movies in a given Year")
    print(whoCreatedMaxMovieWithGivenYear(collections,"2006"))

    # who created the maximum number of  movies for a given genre
    print("who created the maximum number  of movies  for a given genre")
    print(whoCreatedMaxMovieGivenGenres(collections,["Documentary","Short"]))

    # who starred in the maximum number of movies
    print("who starred in the maximum number of movies")
    print(whoStarredMaxNumber(collections))

    # who starred in the maximum number of movies in a given Year
    print("who starred in the maximum number of movies in a given Year")
    print(whoStarredMaxNumberGivenGenres(collections,["Documentary","Short"]))

    print("Find top `N` movies for each genre with the highest IMDB rating")
    moviesWithEachGenreWithHighestImdbRating(collections)

