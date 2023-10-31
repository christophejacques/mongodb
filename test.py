from mongodb import MongoDB


if __name__ == "__main__":   
    # initialisation de l'acces a la mongobd 
    db = MongoDB()

    # boucle sur les bases de donnees
    for database in db.get_database_names():
        db.use_database(database)

        # boucle sur les collections de la bd
        for collection in db.get_collections():
            print("+ ", end="")
            db.use_collection(collection)

            # liste du nombre de documents et des cles
            nb = db.count_documents({})
            print("    > Count documents :", nb)
            if nb:
                cles_document = db.find_one({}).keys()
                print("    > Cles:", list(cles_document))
        print()

# Suivi du lancement du service mongodb gerant la base de donnees
db.use_database("local")
db.use_collection("startup_log")
for doc in db.find({}, {"_id": 0}):
    print(f'- {doc["hostname"]} - {doc["startTime"]}')