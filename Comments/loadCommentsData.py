
import json
from bson import ObjectId

def commentsData(db):
    # read users.json file and convert into list
    file1 = open('/Users/sahilseli/Sigmoid/mongodbWithPython/learning/Comments/comments.json', 'r')
    data = []
    Lines = file1.readlines()
    for line in Lines:
        #changing string to json
        final_dictionary = json.loads(line)
        final_dictionary['_id'] = ObjectId(final_dictionary['_id']['$oid'])
        final_dictionary['movie_id'] = ObjectId(final_dictionary['movie_id']['$oid'])
        final_dictionary['date'] =final_dictionary['date']['$date']['$numberLong']
        data.append(final_dictionary)
    # print(data)

    # Created or Switched to collection
    # names: comments
    Collection = db["comments"]
    if isinstance(data, list):
        Collection.insert_many(data)
    else:
        Collection.insert_one(data)