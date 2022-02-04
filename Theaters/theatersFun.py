
from Theaters.loadTheatersData import theatersData

def insert_theatre(collections,theaterId, location):
    collections.insert_one({'theaterId':theaterId,'location':location})


def top_cities_with_maximum_theatres(collections):
    dic={}
    for i in collections.find():
        city =  i['location']['address']['city']
        if dic.get(city):
            dic[city] +=1
        else:
            dic[city]=1
    a = sorted(dic.items(), key=lambda x: x[1], reverse=True)
    return a[0:10]


def theatres_nearby_given_coordinates(collections,coord):
    dic={}
    for i in collections.find():
        cord_data = i['location']['geo']['coordinates']
        x = float(coord[0]) - float(cord_data[0])
        y = float(coord[1]) - float(cord_data[1])
        x = round(x*x + y*y,5)
        if dic.get(x):
            dic[x].append(i['theaterId'])
        else:
            dic[x]=[]
            dic[x].append(i['theaterId'])
    a = dict(sorted(dic.items()))
    ans = []
    for k,v in a.items():
        ans += v
        if len(ans)+len(v)>10:
            x=10-len(ans)
            ans += v[0:x]
        else:
            ans += v
        if len(ans)>=10:
            break
    return ans



def Theaters(db):
    # for loading data into comments collection
    # theatersData(db)
    collections = db['theaters']

    #insert new theatre
    # insert_theatre(collections,'1999',{'address': {'street1': '11301 W Pico Blvd', 'city': 'Los Angeles', 'state': 'CA', 'zipcode': '90064'}, 'geo': {'type': 'Point', 'coordinates': ['-118.4389', '34.035656']}})

    #top 10 cities with maximum theatre
    top_cities =  top_cities_with_maximum_theatres(collections)
    print("Top 10 cities with maximum theatres")
    print(top_cities)

    # top 10 theatres nearby given coordinates
    nearby_theatre = theatres_nearby_given_coordinates(collections,['-93.24565', '44.85466'])
    print("Top 10 theatres nearby given coordinates eg: ['-93.24565', '44.85466']")
    print(nearby_theatre)

