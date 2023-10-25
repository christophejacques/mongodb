from pymongo import MongoClient


class DataBaseError(BaseException):
    pass


class CollectionError(BaseException):
    pass


class MongoDB:

    client: MongoClient

    def __init__(self, server: str = "", port: int = 0):
        # Configuration par defaut
        if server.strip() == "":
            server = "localhost"
        if port == 0:
            port = 27017

        # Provide the mongodb url to connect python to mongodb using pymongo
        CONNECTION_STRING = f"mongodb://{server}:{str(port)}/"

        # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
        print(f"connecting to : {CONNECTION_STRING}")
        self.client = MongoClient(CONNECTION_STRING)
        self.use_database()

    def use_database(self, dbname: str = ""):
        if dbname:
            if dbname not in self.client.list_database_names():
                raise DataBaseError(f"La base de donnee '{dbname}' n'existe pas.")
            self.database = self.client.get_database(dbname)
            print(f"Using Database : {dbname}")

        else:
            dbname = self.client.list_database_names()[0]
            self.database = self.client.get_database(dbname)

    def get_collections(self):
        return self.database.list_collection_names()

    def get_collection(self, collection_name: str):
        if collection_name not in self.get_collections():
            raise CollectionError(f"Il n'y a pas de collection '{collection_name}' dans la database '{self.database.name}'.")

        self.collection = self.database.get_collection(collection_name)
        return self.collection

    def find(self, query, project: dict = {}):
        if not hasattr(self, "collection"):
            raise CollectionError("Aucune collection n'est selectionnee.")

        return self.collection.find(query, project)

  
if __name__ == "__main__":   
  
    # Get the database
    db = MongoDB()
    db.use_database("tutoriel")

    # print(f"DataBase = {db.name}")
    print("Collections : ", end="")
    print(db.get_collections())

    print()
    db.get_collection("cities")
    query = {"continent": "Asia", "population": {"$gt": 20}}
    print("Query:", query)
    print("Count:", db.collection.count_documents(query))

    for doc in db.find(query, {"_id": 0}):
        print("-", doc)

    print("\nAggregation:")
    for doc in db.collection.aggregate(
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
