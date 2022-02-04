

import json
from bson import ObjectId
def usersData(db):
    # read users.json file and convert into list
    file1 = open('/Users/sahilseli/Sigmoid/mongodbWithPython/learning/Users/users.json', 'r')
    data = []
    Lines = file1.readlines()
    for line in Lines:
        # convert string to json
        final_dictionary = json.loads(line)
        final_dictionary['_id'] = ObjectId(final_dictionary['_id']['$oid'])
        data.append(final_dictionary)


    # Created or Switched to collection
    # names: users
    Collection = db["users"]
    if isinstance(data, list):
        Collection.insert_many(data)
    else:
        Collection.insert_one(data)