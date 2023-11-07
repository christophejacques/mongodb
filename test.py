from mongodb import MongoDB, Collection
from datetime import datetime, timezone
from pprint import pprint
from typing import TypedDict


class Movie(TypedDict):
    name: str
    year: int


if False:
    print(dir(datetime))
    print(dir(timezone))
    help(datetime.fromisoformat)

    initiale = datetime.fromisoformat("2023-11-07T09:14:25.573260+00:00")
    print(initiale, initiale.tzname())
    local = datetime.fromisoformat("2023-11-07T09:14:25.573260+00:00").astimezone()
    print(local, local.tzname())
    print(datetime.now(), datetime.now().astimezone().tzname())
    exit()


db: MongoDB


def ex1():
    db.use_database("tutoriel")
    print(db.get_collection_names())
    print()
    db.use_collection("cities")
    
    print("Liste des index:", db.get_indexe_names())
    print("Liste des champs:", db.get_field_names())
    print(db.find_one())

    print()
    db.use_collection("students")
    print("Liste des index:", db.get_indexe_names())
    print("Liste des champs:", db.get_field_names())
    print(db.find_one())

    db.close()


def ex2():
    db.use_database("tutoriel")
    # db.use_collection("exemple1")
    # print("Find one document in exemple1 :")
    # print(db.find_one())
    # print()

    collection_name = "exemple2"
    # db.drop_collection(collection_name)
    if db.has_collection(collection_name):
        db.use_collection(collection_name)
    else:
        db.create_collection(collection_name)

    print("Contient", db.count_documents(), "documents.")
    print("Insert One Document .. ", end="", flush=True)
    res = db.insert_one({"Nom": "Jacques", 
        "Prenom": "Christophe", 
        "Age": 52, 
        # "date": datetime.isoformat(datetime.now(timezone.utc))})
        "datestr": datetime.isoformat(datetime.now().astimezone()),
        "date": datetime.now()})

    if res.acknowledged:
        print(f"done (id:{res.inserted_id})")
        doc = db.find_one({"_id": res.inserted_id}, {"_id": 0})
        pprint(doc)

        if False:
            date = doc.get("date")
            datestr = doc.get("datestr")
            if isinstance(date, datetime):
                print("> datetime:", date.astimezone(), f"({date.astimezone().tzname()})")
            else:
                print("??? :", date)

            if isinstance(datestr, str):
                date = datetime.fromisoformat(datestr).astimezone()
                print("> string  :", date, f"({date.tzname()})")
            else:
                print("??? :", datestr)

            # print("date:", datetime.fromisoformat(doc.get("date")))
    else:
        print("error")

    print()
    db.close()


def ex3():
    collection: Collection[Movie] = db.test.test

    inserted = collection.insert_one(Movie(name="Jurassic Park", year=1993))
    print("Document enregistre:", inserted.acknowledged)

    result = collection.find_one({"name": "Jurassic Park"})
    print(result)

    assert result is not None
    assert result["year"] == 1993

    # This will raise a type-checking error, despite being present, because it is added by PyMongo.
    assert result["_id"]  # type:ignore[typeddict-item]
    db.close()


def ex4():

    db.close()


if __name__ == "__main__":   
    # initialisation de l'acces a la mongobd 
    db = MongoDB()
    ex1()
