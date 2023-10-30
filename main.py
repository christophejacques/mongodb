from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.cursor import Cursor


class DataBaseError(BaseException):
    pass


class CollectionError(BaseException):
    pass


class MongoDB:

    client: MongoClient
    collection: Collection

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

    def get_database_names(self) -> list[str]:
        return self.client.list_database_names()

    def use_database(self, dbname: str = ""):
        if hasattr(self, "collection"):
            del self.collection

        if dbname:
            if dbname not in self.get_database_names():
                raise DataBaseError(f"La base de donnee '{dbname}' n'existe pas.")
            self.database = self.client.get_database(dbname)
            print(f"Using Database : {dbname}")

        else:
            dbname = self.client.list_database_names()[0]
            self.database = self.client.get_database(dbname)

    def get_collections(self):
        return self.database.list_collection_names()

    def get_collection(self, collection_name: str) -> Collection:
        if collection_name not in self.get_collections():
            raise CollectionError(f"Il n'y a pas de collection '{collection_name}' dans la database '{self.database.name}'.")

        self.collection = self.database.get_collection(collection_name)
        print("Using Collection :", collection_name)
        return self.collection

    def find(self, query, project: dict = {}) -> Cursor:
        if not hasattr(self, "collection"):
            raise CollectionError("Aucune collection n'est selectionnee.")
        
        return self.collection.find(query, project)


if __name__ == "__main__":   
    
    # Get the database
    db = MongoDB()
    db.use_database("tutoriel")

    print("Collections : ", end="")
    print(db.get_collections())

    print()
    db.get_collection("cities")
    print("Indexes count:", len(list(db.collection.list_indexes())))
    for index in db.collection.list_indexes():
        # print(f"- {{'name': {index['name']!r}, 'v': {index['v']}, 'key': {index['key'].get('_id')}}}")
        for cle in index.keys():
            print(f"'{cle}' :", index[cle], end=' (')
            print(index[cle].__class__.__name__, end='), ')

    print()
    print()
    query = {"continent": "Asia", "population": {"$gt": 20}}
    print("Query:", query)
    print("Docs Count:", db.collection.count_documents(query))
    
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
