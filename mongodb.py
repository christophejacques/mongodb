from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.command_cursor import CommandCursor
from pymongo.results import InsertOneResult, InsertManyResult, UpdateResult, DeleteResult
from pymongo.errors import CollectionInvalid

from urllib3.util import parse_url
from typing import Optional
import unittest


def dprint(* args, **kwargs):
    if MongoDB.DEBUG:
        print(*args, **kwargs)


class ConnectionError(BaseException):
    pass


class DataBaseError(BaseException):
    pass


class CollectionError(BaseException):
    pass


class dbClient:

    DEBUG: bool = True
    _client: MongoClient

    def __init__(self, host: str = "", port: Optional[int] = None):
        # Connexion a la base de donnees
        if port is None:
            _, _, server, port, *_ = parse_url(host)
        else:
            server = host

        # Configuration par defaut
        if server is None:
            server = "localhost"

        if port is None:
            port = 27017

        # Provide the mongodb url to connect python to mongodb using pymongo
        CONNECTION_STRING = f"mongodb://{server}:{str(port)}/"

        # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
        dprint(f"connecting to : {CONNECTION_STRING} .. ", flush=True)
        self._client = MongoClient(CONNECTION_STRING, timeoutMS=5000)

    def close(self) -> None:
        # Fermeture de la base de donnees
        dprint("Deconnecting from mongodb .. ", flush=True, end="")
        self._client.close()
        dprint("done")


class dbDatabase(dbClient):

    def has_database(self, dbname: str):
        return dbname in self.get_database_names()

    def create_database(self, dbname: str):
        self._database = self._client[dbname]
        dprint(f"Database {dbname!r} creee.")

    def count_databases(self) -> int:
        # Compte le nombre de Bases de donnees
        return len(self._client.list_database_names())

    def get_database(self, dbname: str):
        # retourne l'objet database correspondant au nom donne
        return self._client.get_database(dbname)

    def get_database_names(self) -> list[str]:
        # Liste les noms des Bases de donnees
        return self._client.list_database_names()

    def use_database(self, dbname: str = ""):
        # Selectionne une Base de donnees
        if hasattr(self, "_collection"):
            del self._collection

        if dbname:
            if dbname not in self.get_database_names():
                raise DataBaseError(f"La base de donnee '{dbname}' n'existe pas.")
            self._database = self._client.get_database(dbname)
            dprint(f"Using Database : {dbname}")

        else:
            dbname = self._client.list_database_names()[0]
            self._database = self._client.get_database(dbname)
            dprint(f"Using default Database : {dbname}")

        return self._database

    def drop_database(self, database=None):
        baseDeDonnees = self._database if database is None else database
        dprint(f"Dropping database {baseDeDonnees!r} .. ", end="", flush=True)
        self._client.drop_database(baseDeDonnees)
        dprint("done.")


class dbCollection(dbDatabase):

    _collection: Collection

    def has_collection(self, collection_name: str) -> bool:
        return collection_name in self.get_collection_names()

    def create_collection(self, collection_name: str, raise_error: bool = True) -> Collection:
        # Creation d'une nouvelle collection
        dprint(f"Creating collection {collection_name!r} .. ", end="", flush=True)
        try:
            self._collection = self._database.create_collection(name=collection_name)

        except CollectionInvalid:
            if raise_error:
                raise
            else:
                self._collection = self._database.get_collection(name=collection_name)
                dprint("existe deja.")

        except Exception:
            dprint("erreur.")
            raise

        else:
            dprint("done.")

        return self._collection

    def rename_collection(self, nouveau_nom: str) -> None:
        # Renomme la collection en cours
        dprint(f"Renaming collection {self._collection.name} to {nouveau_nom!r} .. ", end="", flush=True)
        self._collection.rename(new_name=nouveau_nom)
        dprint("done.")

    def count_collections(self) -> int:
        # Compte le nombre de collections
        return len(self._database.list_collection_names())

    def get_collection_names(self) -> list:
        # Liste le nom des collections
        return self._database.list_collection_names()

    def use_collection(self, collection_name: str) -> Collection:
        # Selectionne une collection
        if collection_name not in self.get_collection_names():
            raise CollectionError(f"Il n'y a pas de collection '{collection_name}' dans la database '{self._database.name}'.")

        self._collection = self._database.get_collection(collection_name)
        dprint("Using Collection :", collection_name)
        return self._collection

    def drop_collection(self, nom_collection: str = "", raise_error: bool = True):
        # Suppression d'une collection
        collection_name = self._collection.name if nom_collection.strip() == "" else nom_collection
        dprint(f"Dropping collection {collection_name!r} .. ", end="", flush=True)
        try:
            nb_docs = self._database.get_collection(collection_name).count_documents({})
            self._database.drop_collection(collection_name)
        except Exception as erreur:
            if raise_error:
                raise
            else:
                dprint("error.")
                dprint(erreur)
        else:
            dprint(f"done. ({nb_docs} docs deleted)")

        if hasattr(self, "collection"):
            del self._collection


class dbIndex(dbCollection):

    def count_indexes(self) -> int:
        # Compte le nombre d'index
        return len(list(self._collection.list_indexes()))

    def get_indexes(self) -> CommandCursor:
        # Liste les Index
        return self._collection.list_indexes()

    def get_indexe_names(self) -> list:
        # Liste les noms des Index
        liste_indexes: list = []

        for index in self._collection.index_information()["_id_"]["key"]:
            liste_indexes.append(index[0])

        return liste_indexes


class dbDocument(dbIndex):

    def insert_one(self, document: dict) -> InsertOneResult:
        # insert un seul document
        return self._collection.insert_one(document)

    def insert_many(self, documents: list[dict]) -> InsertManyResult:
        # insert plusieurs documents
        return self._collection.insert_many(documents)

    def count_documents(self, query: dict = {}) -> int:
        # Compte le nombre de documents d'une collection
        return self._collection.count_documents(query)

    def dictinct(self, key: str, filtre: dict = {}) -> list:
        # ramene la list des valeurs unique de la cle 'key' correspondant au filtre
        return self._collection.distinct(key, filtre)

    def get_all_fields_stats(self) -> dict:
        champs: dict = dict()

        for document in self._collection.find({}):
            for champ in document.keys():
                if champs.get(champ):
                    champs[champ] += 1
                else:
                    champs[champ] = 1
        return champs        

    def get_field_names(self) -> list:
        # Liste les noms des Champs de la collection
        return self.find_one().keys()

    def find_one(self, query: dict = {}, fields: dict = {}):
        # Retourne un seul document d'une collection
        return self._collection.find_one(query, fields)

    def find(self, query, fields: dict = {}) -> Cursor:
        # Retourne tous les documents d'une collection par requete
        if not hasattr(self, "collection"):
            raise CollectionError("Aucune collection n'est selectionnee.")
        
        return self._collection.find(query, fields)

    def update_one(self, filtre: dict, update: dict) -> UpdateResult:
        # Mise à jour d'un seul document
        return self._collection.update_one(filtre, update)

    def update_many(self, filtre: dict, update: dict) -> UpdateResult:
        # Mise à jour des documents correspondants au filtre
        return self._collection.update_one(filtre, update)

    def aggregate(self, pipeline) -> CommandCursor:
        # Retourne tous les documents d'une collection par agggregation
        return self._collection.aggregate(pipeline)

    def delete_one(self, filtre: dict) -> DeleteResult:
        # suppression d'un seul document
        return self._collection.delete_one(filtre)

    def delete_many(self, filtre: dict) -> DeleteResult:
        # suppression de tous les documents correspondants au filtre
        return self._collection.delete_many(filtre)


class MongoDB(dbDocument):

    def __getattribute__(self, name: str):
        # Repartition de la recherche par attribut
        if name.startswith("_"):
            return super().__getattribute__(name)

        elif name in dir(dbDocument):
            return super().__getattribute__(name)

        else:
            print("Recherche database:", name)
            return super().get_database(name)


class MongoDBTest(unittest.TestCase):
    db: MongoDB | None = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print(*args)

    def test1_client(self):
        self.assertIsNotNone(self.db)

    def test2_database(self):
        self.assertIsNotNone(self.db)
        self.assertTrue(self.db.has_database("tutoriel"))
        self.assertEqual(self.db.use_database("tutoriel").name, "tutoriel")

    def test3_collection(self):
        self.assertIsNotNone(self.db)
        self.assertTrue(self.db.has_database("tutoriel"))
        self.assertTrue(self.db.has_collection("cities"))
        self.assertEqual(self.db.use_collection("cities").name, "cities")

    def test4_indexes(self):
        self.assertIsNotNone(self.db)
        self.assertTrue(self.db.has_database("tutoriel"))
        self.assertTrue(self.db.has_collection("cities"))
        self.assertEqual(",".join(self.db.get_indexe_names()), "_id")

    def test5_fields(self):
        self.assertIsNotNone(self.db)
        self.assertTrue(self.db.has_database("tutoriel"))
        self.assertTrue(self.db.has_collection("cities"))
        self.assertEqual("|".join(self.db.get_field_names()), "_id|name|country|continent|population")

    def test9_close(self):
        self.assertIsNotNone(self.db)
        self.db.close()


if __name__ == "__main__":
    MongoDBTest.db = MongoDB()
    unittest.main()
