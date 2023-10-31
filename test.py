from mongodb import MongoDB


if __name__ == "__main__":   
    
    db = MongoDB()

    for database in db.get_database_names():
        db.use_database(database)

        for collection in db.get_collections():
            print("+ ", end="")
            db.use_collection(collection)
            nb = db.count_documents({})
            print("    > Count documents :", nb)
            if nb:
                cles_document = db.find_one({}).keys()
                print("    > Cles:", list(cles_document))
        print()

db.use_database("local")
db.use_collection("startup_log")
for doc in db.find({}, {"_id": 0}):
    print(f'- {doc["hostname"]} - {doc["startTime"]}')
