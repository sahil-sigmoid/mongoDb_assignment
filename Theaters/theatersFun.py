
from Theaters.loadTheatersData import theatersData

def insert_theatre(collections,theaterId, location):
    collections.insert_one({'theaterId':theaterId,'location':location})


def top_cities_with_maximum_theatres(collections):
    res = collections.aggregate([
        {"$project": {"city": "$location.address.city", "theater": "$theaterId"}},
        {"$group": {"_id": {"city": "$city", "theater": "$theater"}, "num": {"$sum": 1}}},
        {"$group": {"_id": "$_id.city", "theaterCount": {"$push": {"theaterName": "$_id.theater", "count": "$num"}}}},
        {"$project": {"_id": 1, "totalTheatersAtCity": {"$sum": "$theaterCount.count"}}},
        {"$sort": {"totalTheatersAtCity": -1}},
        {"$limit": 10}
    ])
    for i in res:
        print(i)


def theatres_nearby_given_coordinates(collections):
    res = collections.aggregate(
        [
            {
                "$geoNear": {
                    "near": {"type": "Point", "coordinates":[-84.526169, 37.986019]},
                    "maxDistance": 10 * 10000000000000,
                    "distanceField": "dist.calculated",
                    "includeLocs": "dist.location",
                    "distanceMultiplier": 1 / 1000,
                    "spherical": "true"
                }
            },
            {"$project": {"city": "$location.address.city", "distance": "$dist.calculated"}},
            {"$group": {"_id": {"distance": "$distance", "city": "$city"}}},
            {"$sort": {"_id.distance": 1}},
            {"$limit": 10}
        ]);
    for i in res:
        print(i)




def Theaters(db):
    # for loading data into comments collection
    # theatersData(db)
    collections = db['theaters']



    #insert new theatre
    # insert_theatre(collections,'1999',{'address': {'street1': '11301 W Pico Blvd', 'city': 'Los Angeles', 'state': 'CA', 'zipcode': '90064'}, 'geo': {'type': 'Point', 'coordinates': ['-118.4389', '34.035656']}})

    #top 10 cities with maximum theatre
    print("Top 10 cities with maximum theatres")
    top_cities_with_maximum_theatres(collections)


    # top 10 theatres nearby given coordinates
    print("Top 10 theatres nearby given coordinates eg: [-84.526169, 37.986019]")
    theatres_nearby_given_coordinates(collections)

