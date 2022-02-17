import json
from bson import ObjectId
# Here i used json package for loading the data
def theatersData(db):
    # read users.json file and convert into list
    file1 = open('/Users/sahilseli/Sigmoid/mongodbWithPython/learning/Theaters/theaters.json', 'r')
    data = []
    Lines = file1.readlines()
    for line in Lines:
        line = json.loads(line.strip())
        if '_id' in line:
            line['_id'] = ObjectId(line['_id']['$oid'])
        if 'theaterId' in line:
            if '$numberInt' in line['theaterId']:
                line['theaterId'] = int(line['theaterId']['$numberInt'])
        if 'location' in line:
            if 'geo' in line['location']:
                if 'coordinates' in line['location']['geo']:
                    line['location']['geo']['coordinates'][0] = float(line['location']['geo']['coordinates'][0]['$numberDouble'])
                    line['location']['geo']['coordinates'][1] = float(line['location']['geo']['coordinates'][1]['$numberDouble'])

        data.append(line)
    # print(data)

    # Created or Switched to collection
    # names: theaters
    Collection = db["theaters"]
    # Collection.drop()
    if isinstance(data, list):
        Collection.insert_many(data)
    else:
        Collection.insert_one(data)