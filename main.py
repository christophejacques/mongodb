from mongodb import MongoDB


if __name__ == "__main__":   
    
    # Get the database
    db = MongoDB()
    print("Databases count:", db.count_databases())
    print("Databases:", db.get_database_names())
    db.use_database("tutoriel")
    print()

    # Get Collections
    print("Collections count:", db.count_collections())
    print("Collections : ", end="")
    print(db.get_collections())

    db.use_collection("cities")
    print()

    # Get Indexes
    print("Indexes Count:", db.count_indexes())
    print("Indexes: ", end="")
    liste_indexes: dict = {}
    for index in db.get_indexes():
        for cle, valeur in index["key"].items():
            liste_indexes[cle] = 0

    print(f"{list(liste_indexes.keys())}")
    print()

    # Get Fields
    cles_document = db.find_one({}, liste_indexes).keys()
    print("Count cles :", len(cles_document))
    print("Cles:", list(cles_document))
    print()
    
    # Get Documents
    print("Count documents :", db.count_documents({}))
    print()
    
    # Get Documents by query
    query = {"continent": "Asia", "population": {"$gt": 20}}
    print("Query:", query)
    print("Docs Count:", db.count_documents(query))
    
    for doc in db.find(query, {"_id": 0}):
        print("-", doc)

    # Get Documents by Aggregation
    print("\nAggregation:")
    for doc in db.aggregate(
     [{"$match": {"continent": {"$in": ['North America', 'Asia']}}},
     {"$sort": {"population": -1}},
     {"$group": {
        "_id": {
          "continent": '$continent',
          "country": '$country'},
        "first_city": {"$first": '$name'},
        "highest_population": {"$max": '$population'}}},
     {"$project": {
        "_id": 0,
        "location": {
          "country": '$_id.country',
          "continent": '$_id.continent'},
        "most_populated_city": {
          "name": '$first_city',
          "population": '$highest_population'}}}]):
        print(doc)
