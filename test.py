from mongodb import MongoDB


db: MongoDB


def ex1():
    db.use_database("tutoriel")
    db.use_collection("cities")

    for methode in dir(db.collection):
        if methode.startswith("_"):
            continue
        print("-", methode)
    
    # help(db.database.validate_collection)
    print(db.find_one({}))
    exit()

    db.use_collection("cities")
    print("Liste des index:", db.get_indexe_names())

    db.use_collection("students")
    print("Liste des index:", db.get_indexe_names())
    db.close()


def ex2():
    db.use_database("tutoriel")
    db.drop_collection("exmple2")
    db.create_collection("exemple2")
    print(db.insert_one({"Nom": "Jacques", "Prenom": "Christophe", "Age": 52}))

    db.close()


if __name__ == "__main__":   
    # initialisation de l'acces a la mongobd 
    db = MongoDB()
    ex2()
