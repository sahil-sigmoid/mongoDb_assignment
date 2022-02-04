import json
from bson import ObjectId
# Here i used json package for loading the data
def theatersData(db):
    # read users.json file and convert into list
    file1 = open('/Users/sahilseli/Sigmoid/mongodbWithPython/learning/Theaters/theaters.json', 'r')
    data = []
    Lines = file1.readlines()
    for line in Lines:
        #changing string to json
        final_dictionary = json.loads(line)
        final_dictionary['_id'] = ObjectId(final_dictionary['_id']['$oid'])
        final_dictionary['theaterId'] = final_dictionary['theaterId']['$numberInt']
        final_dictionary['location']['geo']['coordinates'] = [final_dictionary['location']['geo']['coordinates'][0]['$numberDouble'],final_dictionary['location']['geo']['coordinates'][1]['$numberDouble']]
        data.append(final_dictionary)
    # print(data)

    # Created or Switched to collection
    # names: theaters
    Collection = db["theaters"]
    if isinstance(data, list):
        Collection.insert_many(data)
    else:
        Collection.insert_one(data)