from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.command_cursor import CommandCursor
from pymongo.results import InsertOneResult, InsertManyResult, UpdateResult, DeleteResult


class DataBaseError(BaseException):
    pass


class CollectionError(BaseException):
    pass


class dbDatabase:

    client: MongoClient

    def __init__(self):
        if not hasattr(dbDatabase, "client"):
            print("dbDatabase initialisation .. ", end="")
            dbDatabase.client = None
            print("done.")

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


class dbCollection(dbDatabase):

    collection: Collection

    def __init__(self):
        if not hasattr(dbCollection, "collection"):
            dbDatabase.__init__(self)
            print("dbCollection initialisation .. ", end="")
            dbCollection.collection = None
            print("done.")

    def create_collection(self, collection_name: str) -> Collection:
        self.collection = self.database.create_collection(name=collection_name)
        return self.collection

    def rename_collection(self, nouveau_nom: str) -> None:
        self.collection.rename(new_name=nouveau_nom)

    def count_collections(self) -> int:
        # Compte le nombre de collections
        return len(self.database.list_collection_names())

    def get_collection_names(self) -> list:
        # Liste le nom des collections
        return self.database.list_collection_names()

    def use_collection(self, collection_name: str) -> Collection:
        # Selectionne une collection
        if collection_name not in self.get_collection_names():
            raise CollectionError(f"Il n'y a pas de collection '{collection_name}' dans la database '{self.database.name}'.")

        self.collection = self.database.get_collection(collection_name)
        print("Using Collection :", collection_name)
        return self.collection

    def drop_collection(self, collection_name: str):
        self.database.drop_collection(collection_name)


class dbIndex(dbCollection):

    Index: None

    def __init__(self):
        if not hasattr(dbIndex, "Index"):
            dbCollection.__init__(self)
            print("dbIndexes initialisation .. ", end="")
            dbIndex.Index = None
            print("done.")

    def count_indexes(self) -> int:
        # Compte le nombre d'index
        return len(list(self.collection.list_indexes()))

    def get_indexes(self) -> CommandCursor:
        # Liste les Index
        return self.collection.list_indexes()

    def get_indexe_names(self) -> list:
        # Liste les noms des Index
        liste_indexes: list = []

        for index in self.collection.index_information()["_id_"]["key"]:
            liste_indexes.append(index[0])

        return liste_indexes


class dbDocument(dbIndex):

    Document: None

    def __init__(self):
        if not hasattr(dbDocument, "Document"):
            dbIndex.__init__(self)
            print("dbDocument initialisation .. ", end="")
            dbDocument.Document = None
            print("done.")

    def insert_one(self, document: dict) -> InsertOneResult:
        # insert un seul document
        return self.collection.insert_one(document)

    def insert_many(self, documents: list[dict]) -> InsertManyResult:
        # insert plusieurs documents
        return self.collection.insert_many(documents)

    def count_documents(self, query: dict = {}) -> int:
        # Compte le nombre de documents d'une collection
        return self.collection.count_documents(query)

    def dictinct(self, key: str, filtre: dict = {}) -> list:
        # ramene la list des valeurs unique de la cle 'key' correspondant au filtre
        return self.collection.distinct(key, filtre)

    def find_one(self, query: dict = {}, fields: dict = {}):
        # Retourne un seul document d'une collection
        return self.collection.find_one(query, fields)

    def find(self, query, fields: dict = {}) -> Cursor:
        # Retourne tous les documents d'une collection par requete
        if not hasattr(self, "collection"):
            raise CollectionError("Aucune collection n'est selectionnee.")
        
        return self.collection.find(query, fields)

    def update_one(self, filtre: dict, update: dict) -> UpdateResult:
        # Mise à jour d'un seul document
        return self.collection.update_one(filtre, update)

    def update_many(self, filtre: dict, update: dict) -> UpdateResult:
        # Mise à jour des documents correspondants au filtre
        return self.collection.update_one(filtre, update)

    def aggregate(self, pipeline) -> CommandCursor:
        # Retourne tous les documents d'une collection par agggregation
        return self.collection.aggregate(pipeline)

    def delete_one(self, filtre: dict) -> DeleteResult:
        # suppression d'un seul document
        return self.collection.delete_one(filtre)

    def delete_many(self, filtre: dict) -> DeleteResult:
        # suppression de tous les documents correspondants au filtre
        return self.collection.delete_many(filtre)


class MongoDB(dbDocument):

    def __init__(self, server: str = "", port: int = 0):
        # Configuration par defaut
        if server.strip() == "":
            server = "localhost"
        if port == 0:
            port = 27017

        # Provide the mongodb url to connect python to mongodb using pymongo
        CONNECTION_STRING = f"mongodb://{server}:{str(port)}/"

        # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
        print(f"connecting to : {CONNECTION_STRING} .. ", flush=True)
        self.client = MongoClient(CONNECTION_STRING, timeoutMS=5000)
        self.use_database()

    def close(self) -> None:
        print("Deconnecting from mongodb .. ", flush=True, end="")
        self.client.close()
        print("done")


if __name__ == "__main__":
    # assert isinstance(dbDatabase, type)
    # assert isinstance(dbDatabase(), dbDatabase)
    # assert isinstance(dbCollection, type)
    # assert isinstance(dbCollection(), dbCollection)
    # assert isinstance(dbIndex, type)
    # assert isinstance(dbIndex(), dbIndex)
    assert isinstance(dbDocument, type)
    assert isinstance(dbDocument(), dbDocument)

    print("Compilation OK")
