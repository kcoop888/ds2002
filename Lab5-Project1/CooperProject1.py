import numpy
import csv
import json
import sqlite3
import pandas as pd
import datetime


def getDesiredFormat():
    valid = False
    d_f = ""
    while not valid:
        d_f = input("What is the desired format? CSV, JSON, or SQL?").lower()

        if d_f == "csv" or d_f == "json" or d_f == "sql":
            valid = True
        else:
            print("I didn't get that. Try again.")
    return d_f


def getDesiredOutput():
    valid = False
    while not valid:
        do = input("What is the desired output? Print or File?").lower()

        if do == "print" or do == "file":
            valid = True
            if do == "file":
                YoN = input("This will store it in a file and not display. Is that alright? (y/n)").lower()
                if YoN == "n":
                    valid = False
        else:
            print("I didn't get that. Try again.")
    return do


def getClean():
    valid = False
    gc = ""
    while not valid:
        gc = input("Would you like the columns collapsed? (y/n)").lower()

        if gc == "y" or gc == "n":
            valid = True
        else:
            print("I didn't get that. Try again.")
    return gc


def getData(gc):
    data = pd.read_csv(r'games-features-edit.csv')
    df_data = pd.DataFrame(data)
    if gc == "y":
        genre_cols = [
             "GenreIsNonGame", "GenreIsIndie", "GenreIsAction", "GenreIsAdventure",
             "GenreIsCasual", "GenreIsStrategy", "GenreIsRPG", "GenreIsSimulation", "GenreIsEarlyAccess",
             "GenreIsFreeToPlay", "GenreIsSports", "GenreIsRacing", "GenreIsMassivelyMultiplayer"
        ]
        other_cols = ["ResponseName", "ReleaseDate", "Metacritic", "RecommendationCount", "IsFree", "PriceInitial"]
        df_data = df_data.drop_duplicates()
        df_data = df_data.melt(id_vars=other_cols, var_name="Genre")\
            .query('value == 1').drop(columns=["value"]).drop_duplicates()

        # print(df.head)
    return df_data


def formatCSV(do, df_data):
    if do == "file":
        print("Making a duplicate CSV file.")
        df_data.to_csv('new_CSV.csv', index=False)
    else:
        print(df_data.to_csv(index=False))


def formatJSON(do, df_data):
    ret_json = df_data.to_json(orient='values')
    if do == "file":
        print("Making a file.")
        df_data.to_json("new_JSON.json", orient='values')
    else:
        print(ret_json)


def formatSQL(do, df_data, gc):
    conn = sqlite3.connect('games.db')
    cur = conn.cursor()
    cur.execute("""DELETE FROM videogames;""")
    cur.execute("""DROP TABLE IF EXISTS videogames;""")
    conn.commit()

    if gc == "y":
        cur.execute("""
                                CREATE TABLE videogames(
                                ResponseName TEXT,
                                ReleaseDate TEXT,
                                Metacritic INT,
                                RecommendationCount INT,
                                IsFree BOOLEAN,
                                PriceInitial FLOAT,
                                Genre TEXT
                                )"""
                    )
        conn.commit()
        print("I'm working but it might take a few minutes...")

        for row in df_data.itertuples():
            game = [
                row.ResponseName, row.ReleaseDate, row.Metacritic, row.RecommendationCount,
                row.IsFree, row.PriceInitial, row.Genre
            ]
            # print(game)
            # (ResponseName, ReleaseDate, Metacritic, RecommendationCount,
            #  IsFree, GenreIsNonGame, GenreIsIndie, GenreIsAction, GenreIsAdventure,
            #  GenreIsCasual, GenreIsStrategy, GenreIsRPG, GenreIsSimulation, GenreIsEarlyAccess,
            #  GenreIsFreeToPlay, GenreIsSports, GenreIsRacing, GenreIsMassivelyMultiplayer,
            #  PriceInitial)
            cur.execute("INSERT INTO videogames VALUES(?,?,?,?,?,?,?);",
                        game
                        )
            # print(row.ResponseName, row.ReleaseDate)
            conn.commit()
    else:
        cur.execute("""
                        CREATE TABLE videogames(
                        ResponseName TEXT,
                        ReleaseDate TEXT,
                        Metacritic INT,
                        RecommendationCount INT,
                        IsFree BOOLEAN,
                        GenreIsNonGame BOOLEAN,
                        GenreIsIndie BOOLEAN,
                        GenreIsAction BOOLEAN,
                        GenreIsAdventure BOOLEAN,
                        GenreIsCasual BOOLEAN,
                        GenreIsStrategy BOOLEAN,
                        GenreIsRPG BOOLEAN,
                        GenreIsSimulation BOOLEAN,
                        GenreIsEarlyAccess BOOLEAN,
                        GenreIsFreeToPlay BOOLEAN,
                        GenreIsSports BOOLEAN,
                        GenreIsRacing BOOLEAN,
                        GenreIsMassivelyMultiplayer BOOLEAN,
                        PriceInitial FLOAT
                        )"""
                    )
        conn.commit()

        print("I'm working but it might take a few minutes...")

        for row in df_data.itertuples():
            game = [
                row.ResponseName, row.ReleaseDate, row.Metacritic, row.RecommendationCount,
                row.IsFree, row.GenreIsNonGame, row.GenreIsIndie, row.GenreIsAction,
                row.GenreIsAdventure, row.GenreIsCasual, row.GenreIsStrategy, row.GenreIsRPG,
                row.GenreIsSimulation, row.GenreIsEarlyAccess, row.GenreIsFreeToPlay,
                row.GenreIsSports, row.GenreIsRacing, row.GenreIsMassivelyMultiplayer,
                row.PriceInitial
            ]
            # print(game)
            # (ResponseName, ReleaseDate, Metacritic, RecommendationCount,
            #  IsFree, GenreIsNonGame, GenreIsIndie, GenreIsAction, GenreIsAdventure,
            #  GenreIsCasual, GenreIsStrategy, GenreIsRPG, GenreIsSimulation, GenreIsEarlyAccess,
            #  GenreIsFreeToPlay, GenreIsSports, GenreIsRacing, GenreIsMassivelyMultiplayer,
            #  PriceInitial)
            cur.execute("INSERT INTO videogames VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",
                        game
                        )
            # print(row.ResponseName, row.ReleaseDate)
            conn.commit()

    if do == "file":
        print("The database is stored in games.db!")
    else:
        cur.execute("SELECT * FROM videogames;")
        all_results = cur.fetchall()
        print(all_results)


def main():
    # desired_format = ""
    # desired_output = ""

    desired_format = getDesiredFormat()
    desired_output = getDesiredOutput()
    desired_clean = getClean()

    df = getData(desired_clean)

    if desired_format == "csv":
        formatCSV(desired_output, df)
    elif desired_format == "json":
        formatJSON(desired_output, df)
    elif desired_format == "sql":
        formatSQL(desired_output, df, desired_clean)


if __name__ == "__main__":
    main()
