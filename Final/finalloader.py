import pandas as pd
import pymongo


def connectToMongo(debug):
    if debug:
        print("\nOpening connection!")
    host_name = "localhost"
    port = "27017"

    conn_str = {"local": f"mongodb://{host_name}:{port}/"}
    client = pymongo.MongoClient(conn_str["local"])

    if debug:
        print(f"Local Connection String: {conn_str['local']}\n")

    return client


def create_db(client, all_data, debug):
    db = client["final_proj"]
    netflix = db['netflix']

    if "netflix" not in db.list_collection_names():
        netflix.insert_many(all_data.to_dict('records'))
        if debug:
            print("Database Names: ", client.list_database_names())  # [no blog]
            print("Collection Names: ", db.list_collection_names())  # []
    else:
        should_reset = input("Netflix is already in the collections. Should we reset the data? (y/n)")
        if should_reset.strip().lower()[0] == "y":
            print("Resetting db['netflix']")
            temp = db.temp
            db.drop_collection("netflix")
            netflix = db['netflix']
            db.drop_collection(temp)
            netflix.insert_many(all_data.to_dict('records'))
            if debug:
                print("Database Names: ", client.list_database_names())  # [no blog]
                print("Collection Names: ", db.list_collection_names())  # []
        else:
            print("Did not reset data.")
    return db


def getData(debug):
    # which cols do I want?
    # id, title, is_movie (type refactored to binary), release_year, age_certification, runtime,
    # genres, production_countries, number_of_seasons, directors, score, is_best, is_good

    # Best Movie by Year
    bmby = pd.read_csv("Best Movie by Year Netflix.csv")
    bmby.drop(['index', 'RELEASE_YEAR', 'SCORE', 'MAIN_GENRE', 'MAIN_PRODUCTION'], axis=1, inplace=True)
    bmby.rename(columns={"TITLE": "title"}, inplace=True)
    bmby['is_best'] = 2
    # Best Movies
    bms = pd.read_csv("Best Movies Netflix.csv")
    bms.drop(['index', 'RELEASE_YEAR', 'SCORE', 'NUMBER_OF_VOTES', 'DURATION', 'MAIN_GENRE', 'MAIN_PRODUCTION'],
             axis=1, inplace=True)
    bms.rename(columns={"TITLE": "title"}, inplace=True)
    bms['is_good'] = 1
    # Best Show by Year
    bsby = pd.read_csv("Best Show by Year Netflix.csv")
    bsby.drop(['index', 'RELEASE_YEAR', 'SCORE', 'NUMBER_OF_SEASONS', 'MAIN_GENRE', 'MAIN_PRODUCTION'],
              axis=1, inplace=True)
    bsby.rename(columns={"TITLE": "title"}, inplace=True)
    bsby['is_best'] = 2
    # Best Shows
    bss = pd.read_csv("Best Shows Netflix.csv")
    bss.drop(['index', 'RELEASE_YEAR', 'SCORE', 'NUMBER_OF_VOTES', 'DURATION', 'NUMBER_OF_SEASONS', 'MAIN_GENRE',
              'MAIN_PRODUCTION'], axis=1, inplace=True)
    bss.rename(columns={"TITLE": "title"}, inplace=True)
    bss['is_good'] = 1
    # Raw Credits
    rc = pd.read_csv("raw_credits.csv")
    rc.drop(['index',  'person_id', 'character'], axis=1, inplace=True)
    directors = rc[rc.role == "DIRECTOR"].copy()
    directors.drop(['role'], axis=1, inplace=True)
    directors.rename(columns={"name": "directors"}, inplace=True)
    directors = directors.groupby(['id'])['directors'].apply(' and '.join).reset_index()
    # Raw Titles
    rt = pd.read_csv("raw_titles.csv")
    rt.rename(columns={"type": "is_movie"}, inplace=True)

    all_info = rt.merge(bmby, on='title', how="left").merge(bms, on='title', how="left")\
        .merge(bsby, on='title', how="left").merge(bss, on='title', how="left")\
        .merge(directors, on='id', how="left")
    # best_cols = ['is_best_x', 'is_best_y']
    # good_cols = ['is_good_x', 'is_good_y']
    both_cols = ['is_best_x', 'is_best_y', 'is_good_x', 'is_good_y']
    all_info['how_good'] = all_info[both_cols].sum(1)
    # all_info['how_good'] = all_info['is_best_x'].str.cat(all_info['is_best_y'], na_rep='')\
    #     .str.cat(all_info['is_good_x'], na_rep='').str.cat(all_info['is_good_y'], na_rep='')
    all_info.drop(both_cols, axis=1, inplace=True)
    other_cols = ["age_certification", "genres", "id", "imdb_id", "imdb_votes", "index", "production_countries"]
    all_info.drop(other_cols, axis=1, inplace=True)
    all_info['title'] = all_info['title'].str.lower()
    all_info = all_info.drop_duplicates(subset=["title"])

    if debug:
        print("Best Movie by Year info\n", bmby.columns)
        print("Best Movies info\n", bms.columns)
        print("Best Show by Year info\n", bsby.columns)
        print("Best Shows info\n", bss.columns)
        print("credits info\n", rc.columns)
        print("diretors info\n", directors.columns)
        print("titles info\n", rt.columns)
        print("all info info\n", all_info.columns)
        with pd.option_context('display.max_rows', None, 'display.max_columns', 6):  # number could be None
            print(all_info)
        # print(all_info)

    return all_info


def main():
    client = connectToMongo(False)
    all_info = getData(False)
    db = create_db(client, all_info, False)

    if "netflix" in db.list_collection_names():
        print("Netflix successfully loaded in the db with", db["netflix"].count_documents({}), "documents")


if __name__ == '__main__':
    main()
