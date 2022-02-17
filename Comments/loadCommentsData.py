
import json
from bson import ObjectId
from datetime import datetime
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
        x =final_dictionary['date']['$date']['$numberLong']
        datetime_obj = datetime.fromtimestamp(int(x) / 1e3)
        final_dictionary['date']=datetime_obj
        data.append(final_dictionary)
    # print(data)

    # Created or Switched to collection
    # names: comments
    Collection = db["comments"]
    # Collection.drop()
    if isinstance(data, list):
        Collection.insert_many(data)
    else:
        Collection.insert_one(data)