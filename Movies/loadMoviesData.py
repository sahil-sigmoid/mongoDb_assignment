import json
from bson import ObjectId


def moviesData(db):
    # read users.json file and convert into list
    file1 = open('/Users/sahilseli/Sigmoid/mongodbWithPython/learning/Movies/movies.json', 'r')
    data = []
    Lines = file1.readlines()
    for line in Lines:
        #changing string to json
        final_dictionary = json.loads(line)
        if(final_dictionary.get('_id')):
            final_dictionary['_id'] = ObjectId(final_dictionary['_id']['$oid'])
            if(final_dictionary.get('year')):
                x=final_dictionary['year']
                if type(x)!=str:
                    final_dictionary['year'] = final_dictionary['year']['$numberInt']

            if(final_dictionary.get('runtime')):
                 final_dictionary['runtime'] = final_dictionary['runtime']['$numberInt']

            if(final_dictionary.get('released')):
                final_dictionary['released'] = final_dictionary['released']['$date']['$numberLong']

            x = final_dictionary['imdb']['rating']
            if type(x)!=str and final_dictionary['imdb']['rating'].get('$numberDouble'):
                final_dictionary['imdb']['rating'] = final_dictionary['imdb']['rating']['$numberDouble']
            elif type(x)!=str:
                final_dictionary['imdb']['rating'] = final_dictionary['imdb']['rating']['$numberInt']

            if(final_dictionary['imdb'].get('votes')):
                 final_dictionary['imdb']['votes'] = final_dictionary['imdb']['votes']['$numberInt']
            final_dictionary['imdb']['id'] = final_dictionary['imdb']['id']['$numberInt']

            if final_dictionary.get('tomatoes'):
                if(final_dictionary['tomatoes'].get('viewer')):
                    if(final_dictionary['tomatoes']['viewer'].get('rating')):
                        if final_dictionary['tomatoes']['viewer']['rating'].get('$numberInt'):
                            final_dictionary['tomatoes']['viewer']['rating'] = final_dictionary['tomatoes']['viewer']['rating']['$numberInt']
                        else :
                            final_dictionary['tomatoes']['viewer']['rating'] = final_dictionary['tomatoes']['viewer']['rating']['$numberDouble']

                    final_dictionary['tomatoes']['viewer']['numReviews'] = final_dictionary['tomatoes']['viewer']['numReviews']['$numberInt']
                final_dictionary['tomatoes']['lastUpdated']=final_dictionary['tomatoes']['lastUpdated']['$date']['$numberLong']

            if final_dictionary.get('num_mflix_comments'):
                final_dictionary['num_mflix_comments'] = final_dictionary['num_mflix_comments']['$numberInt']
        # print(final_dictionary)
        data.append(final_dictionary)
        # break;
    # print(data)

    # Created or Switched to collection
    # names: movies
    Collection = db["movies"]
    if isinstance(data, list):
        Collection.insert_many(data)
    else:
        Collection.insert_one(data)