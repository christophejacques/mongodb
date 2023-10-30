from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.command_cursor import CommandCursor
from typing import Union, Any


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

    def count_databases(self) -> int:
        # Compte le nombre de Bases de donnees
        return len(self.client.list_database_names())

    def get_database_names(self) -> list[str]:
        # Liste les noms des Bases de donnees
        return self.client.list_database_names()

    def use_database(self, dbname: str = ""):
        # Selectionne une Base de donnees
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

    def count_collections(self) -> int:
        # Compte le nombre de collections
        return len(self.database.list_collection_names())

    def get_collections(self) -> list:
        # Liste le nom des collections
        return self.database.list_collection_names()

    def use_collection(self, collection_name: str) -> Collection:
        # Selectionne une collection
        if collection_name not in self.get_collections():
            raise CollectionError(f"Il n'y a pas de collection '{collection_name}' dans la database '{self.database.name}'.")

        self.collection = self.database.get_collection(collection_name)
        print("Using Collection :", collection_name)
        return self.collection

    def count_indexes(self) -> int:
        # Compte le nombre d'index
        return len(list(db.collection.list_indexes()))

    def get_indexes(self) -> CommandCursor:
        # Liste les Index
        return db.collection.list_indexes()

    def count_documents(self, query) -> int:
        # Compte le nombre de documents d'une collection
        return self.collection.count_documents(query)

    def find_one(self, query, fields: dict = {}):
        # Retourne un seul document d'une collection
        return self.collection.find_one(query, fields)

    def find(self, query, fields: dict = {}) -> Cursor:
        # Retourne tous les documents d'une collection par requete
        if not hasattr(self, "collection"):
            raise CollectionError("Aucune collection n'est selectionnee.")
        
        return self.collection.find(query, fields)

    def aggregate(self, pipeline) -> CommandCursor:
        # Retourne tous les documents d'une collection par agggregation
        return self.collection.aggregate(pipeline)


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
    LS_Res: str = ""
    liste_indexes: dict = {}
    for index in db.get_indexes():
        for cle, valeur in index["key"].items():
            LS_Res += f"{cle}, "
            liste_indexes[cle] = 0

    print(f"[{LS_Res[:-2]}]")

    print()
    # Get Fields
    cles_document = db.find_one({}, liste_indexes).keys()
    print("Count cles :", len(cles_document))
    print("Cles:", list(cles_document))

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
