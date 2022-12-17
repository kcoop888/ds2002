import pymongo
from datetime import date
import re
from text2digits import text2digits


def connectToMongo(debug):
    if debug:
        print("\nOpening connection!")
    host_name = "localhost"
    port = "27017"

    conn_str = {"local": f"mongodb://{host_name}:{port}/"}
    client = pymongo.MongoClient(conn_str["local"])

    if debug:
        print(f"Local Connection String: {conn_str['local']}\n")

    db = client["final_proj"]
    netflix = db["netflix"]

    return client, netflix


# def queryMongo(netflix):
    # netflix.find_one({})
    # pprint.pprint(netflix.find_one({"title": "breaking bad"}))
    # pprint.pprint(posts.find_one({"author": "Mike"}))
    # pprint.pprint(posts.find_one({"_id": post_id}))
    # # MongoDb function for SELECT * FROM posts
    # for post in posts.find():
    #     pprint.pprint(post)
    # # MongoDb function for SELECT * FROM posts WHERE author = 'Mike'
    # for post in posts.find({"author": "Mike"}):
    #     pprint.pprint(post)
    #
    # print("All Docs: ", posts.count_documents({}))  # 3
    # print("Matching Docs: ", posts.count_documents({"author": "Mike"}))  # 2
    #
    # # MongoDb function for SELECT * FROM posts WHERE
    # #   date < '11/12/2009 at 12.0.0:00' ORDER BY author
    # d = datetime.datetime(2009, 11, 12, 12)
    # for post in posts.find({"date": {"$lt": d}}).sort("author"):
    #     pprint.pprint(post)


def questions():
    """
    how many movies total
    how many good movies
    who was the director in movie
    which movies were by director
    what were top 5 shows on netflix x years ago
        show me the top 5 shows on netflix 2 years ago
        show me the top x shows on netflix two years ago
    what was the top movie on netflix in yr
    how long was the best movie on netflix last year (year - 1)
    what was the release year of that movie
    what was the mean score of the year x



    """
    qs = ["In this list, the '/' means use either option, not both.",
          "How many movies/shows are there?",
          "How many good/best movies/shows are there?",
          "Who was/Show me the director of (insert title)?",
          "What were/Show me the top/bottom # movies/shows # years ago?",
          "What were/Show me the top/bottom # movies/shows last/this year?",
          "What was/Show me the top/bottom movie/show of (insert year)?",
          "What was/Show me the top/bottom movie/show # years ago?",
          "What was/Show me the top/bottom movie/show last/this year?",
          "How long is (insert title)?",
          "How long was the top/bottom movie/show of (insert year)?",
          "How long was the top/bottom movie/show # years ago?",
          "How long was the top/bottom movie/show last/this year?",
          "When was (insert title) released?",
          "What was/Show me the mean score of (insert year)?",
          "What was/Show me the mean score of the year (insert year)?",
          "How many seasons did (insert title) have?"
          ]
    for q in qs:
        print(q)


def cleanInput(statement, debug):
    statement = statement.lower().strip()
    if len(statement) > 1 and (statement[-1] == "?" or statement[-1] == "."):
        statement = statement[:-1].strip()

    t2d = text2digits.Text2Digits()
    converted = t2d.convert(statement)

    if debug:
        print(converted)
    return converted


def how_many_good_movies(netflix, q1):
    word1 = q1.group(1)
    word2 = q1.group(3)
    # print(word1, "'"+word2+"'")
    if word1 == "movie" or word1 == "show":
        word1 = word1 + "s"
    if word1 == "movies" or word1 == "shows":
        print("There are", netflix.count_documents({"is_movie": word1[:-1].upper()}), "total", word1 + ".")
    else:
        if word2 == "movie" or word2 == "show":
            word2 = word2 + "s"
        if word1 == "best":
            print("There are", netflix.count_documents({"is_movie": word2[:-1].upper(),
                                                        "how_good": {"$gt": 1}}), word1, word2 + ".")
        elif word1 == "good":
            print("There are", netflix.count_documents({"is_movie": word2[:-1].upper(),
                                                        "how_good": {"$gt": 0}}), word1, word2 + ".")
        else:
            print("There are", netflix.count_documents({"is_movie": word2[:-1].upper(),
                                                        "how_good": 0}), "not good", word2 + ".")


def who_directed(netflix, q2):
    # print(q2.group(1), "|", q2.group(2), "|", q2.group(3), "|", q2.group(4), "|", q2.group(5), "|")
    dr = netflix.find_one({"title": q2.group(5)})
    # print(dr)
    if dr is None:
        print("I don't know the director in", q2.group(5).title() + ".")
        return
    dr = dr.get("directors")
    # print(type(dr), dr, type(str(dr)), str(dr))
    if str(dr) == "nan":
        print("I don't know the director in", q2.group(5).title() + ".")
        return
    # print(dr)
    print("The director in", q2.group(5).title(), "was", dr + ".")


def top_with_year(netflix, medtype, find_yr):
    if medtype == "movie" or medtype == "show":
        medtype = medtype + "s"

    return netflix.find_one({"is_movie": medtype[:-1].upper(), "release_year": find_yr, "how_good": {"$gt": 1}})


def how_many_top_with_year(netflix, medtype, length, find_yr=None):
    better_mos = []
    if medtype == "movie" or medtype == "show":
        medtype = medtype + "s"
    if find_yr is None:
        finder = netflix.find({"is_movie": medtype[:-1].upper()})
    else:
        finder = netflix.find({"is_movie": medtype[:-1].upper(), "release_year": find_yr})
    for mos in finder:
        # print(mos)
        if len(better_mos) < int(length):
            better_mos.append(mos)
        else:
            temp = mos
            for item in better_mos:
                if item.get("imdb_score") < temp.get("imdb_score"):
                    better_mos.remove(item)
                    better_mos.append(temp)
                    temp = item

    return better_mos


def how_many_bottom_with_year(netflix, medtype, length, find_yr=None):
    worst_mos = []
    if medtype == "movie" or medtype == "show":
        medtype = medtype + "s"
    if find_yr is None:
        finder = netflix.find({"is_movie": medtype[:-1].upper()})
    else:
        finder = netflix.find({"is_movie": medtype[:-1].upper(), "release_year": find_yr})
    # print(finder)
    for mos in finder:
        # print(mos)
        if len(worst_mos) < int(length):
            worst_mos.append(mos)
        else:
            temp = mos
            for item in worst_mos:
                if item.get("imdb_score") > temp.get("imdb_score"):
                    worst_mos.remove(item)
                    worst_mos.append(temp)
                    temp = item
    return worst_mos


def duration(netflix, medtype=None, title=None):
    if medtype is None and title is not None:
        return netflix.find_one({"title": title})


def released(netflix, title):
    return netflix.find_one({"title": title})


def getMean(netflix, medtype, find_yr):
    if medtype is None:
        # print(find_yr)
        finder = netflix.find({"release_year": find_yr})
    else:
        # print(medtype, find_yr)
        finder = netflix.find({"is_movie": medtype.upper(), "release_year": find_yr})
    # finder.rewind()
    total_score = 0
    counter = 0.0
    # if finder is None:
    #     print("Could not find a movie or show with that year.")
    #     return -1
    for mos in finder:
        # print(float(mos.get('imdb_score')))
        # print("hit mos")
        if str(mos.get('imdb_score')) != "nan":
            counter += 1.0
            total_score += float(mos.get('imdb_score'))
    if counter == 0:
        return -1
    mean = total_score / counter

    return mean


def getSeasons(netflix, title):
    mos = netflix.find_one({"title": title})
    if mos.get("is_movie") == "MOVIE":
        return -1
    return mos.get("seasons")


def chatbot(netflix, debug):
    while True:
        question = input("\nAsk Away! If you don't know what to ask, say 'help'. Say 'quit' to stop.\n")
        if question.lower().strip()[:4] == "quit":
            break

        question = cleanInput(question, debug)
        # Then use a regex to look for a word there or no word there.
        # search the db for words with that in the right column
        q1 = re.search(r'^how many (\w+)( (\w*) *(\w)*)?', question)
        q2 = re.search(r'^(\w* )*the director ((of)|(in)) (\w+.*)+', question)
        q3 = re.search(r'^((what were)|(show me)) the top (\d+) (\w+)( (\w* )?(\d)?(\w+| *)*)*', question)
        q4 = re.search(r'^((what were)|(show me)) the bottom (\d+) (\w+)( (\w* )?(\d)?(\w+| *)*)*', question)
        q5 = re.search(r'^((what was)|(show me)) the top (\w+)( (of|in)? ?((\d+)|(\w+ ?)+))*', question)
        q6 = re.search(r'^((what was)|(show me)) the bottom (\w+)( (of|in)? ?((\d+)|(\w+ ?)+))*', question)
        q7 = re.search(r'^how long (was|is)( the top| the bottom)? ((\w+)( (of|in)? ?((\d+)|(\w+ ?)+))*)', question)
        q8 = re.search(r'when was (\w+.*)+ released', question)
        q9 = re.search(r'show me when (\w+.*)+ was released', question)
        q10 = re.search(r'(what was|show me) the mean score of (\w+ in )?(the year )?(\d+)', question)
        q11 = re.search(r'.* seasons (did|does) (\w+.*) have', question)
        if question[:4] == "help":
            print("You can ask:")
            questions()
        elif q11 is not None:
            seasons = getSeasons(netflix, q11.group(2))
            if seasons == -1:
                print(f"{q11.group(2).title()} does not have seasons.")
                continue
            response = f"{q11.group(2).title()} has {seasons} seasons."
            print(response)
        elif q1 is not None:
            how_many_good_movies(netflix, q1)
        elif q2 is not None:
            who_directed(netflix, q2)
        elif q3 is not None:
            # q3 = re.search(r'^((what were)|(show me)) the top (\d+) (\w+)( (\w* )?(\d)?(\w+| *)*)*', question)
            # print(q3.group(4), "|", q3.group(5), "|", q3.group(6), "|", q3.group(7), "|", q3.group(8))
            if q3.group(7) is None:
                better_mos = how_many_top_with_year(netflix, q3.group(5), q3.group(4))

                if len(better_mos) <= 0:
                    print("Could not find a movie or show with that year.")
                    continue
                response = f"The top {q3.group(4)} {q3.group(5)} were "
                for mos in better_mos:
                    response += mos.get("title").title() + ", "
                response = response[:-2] + "."
                print(response)
            else:
                yr = date.today().year
                if q3.group(7).__contains__("last"):
                    find_yr = yr - 1
                elif q3.group(7).__contains__("this"):
                    find_yr = yr
                else:
                    find_yr = yr - int(q3.group(7))

                better_mos = how_many_top_with_year(netflix, q3.group(5), q3.group(4), find_yr)

                if len(better_mos) <= 0:
                    print("Could not find a movie or show with that year.")
                    continue

                if q3.group(7).__contains__("last"):
                    response = f"The top {q3.group(4)} {q3.group(5)} last year were "
                else:
                    response = f"The top {q3.group(4)} {q3.group(5)} in {find_yr} were "
                for mos in better_mos:
                    response += mos.get("title").title() + ", "
                response = response[:-2] + "."
                print(response)
        elif q4 is not None:
            # print(q4.group(4), "|", q4.group(5), "|", q4.group(6), "|", q4.group(7), "|", q4.group(8))
            if q4.group(7) is None:
                worst_mos = how_many_bottom_with_year(netflix, q4.group(5), q4.group(4))

                response = f"The bottom {q4.group(4)} {q4.group(5)} were "
                for mos in worst_mos:
                    response += mos.get("title").title() + ", "
                response = response[:-2] + "."
                print(response)
            else:
                yr = date.today().year
                if q4.group(7).__contains__("last"):
                    find_yr = yr - 1
                elif q4.group(7).__contains__("this"):
                    find_yr = yr
                else:
                    find_yr = yr - int(q4.group(7))

                worst_mos = how_many_bottom_with_year(netflix, q4.group(5), q4.group(4), find_yr)
                if q4.group(7).__contains__("last"):
                    response = f"The bottom {q4.group(4)} {q4.group(5)} last year were "
                else:
                    response = f"The bottom {q4.group(4)} {q4.group(5)} in {find_yr} were "
                for mos in worst_mos:
                    response += mos.get("title").title() + ", "
                response = response[:-2] + "."
                print(response)
        elif q5 is not None:
            # print(q5.group(4), "|", q5.group(5), "|", q5.group(6), "|", q5.group(7), "|", q5.group(8))
            # , "|", q5.group(9), "|", q5.group(10), "|", q5.group(11), "|", q5.group(12), "|")
            if q5.group(5) is None:
                better_mos = how_many_top_with_year(netflix, q5.group(4), 1)
                if len(better_mos) <= 0:
                    print("Could not find a movie or show with that year.")
                    continue
                response = f"A {q5.group(4)} with the highest rating was {better_mos[0].get('title').title()}."
                print(response)
            else:
                yr = date.today().year
                if q5.group(7).__contains__("last"):
                    find_yr = yr - 1
                elif q5.group(7).__contains__("this"):
                    find_yr = yr
                elif q5.group(7).__contains__("ago"):
                    find_yr = yr - int(q5.group(8))
                else:
                    find_yr = int(q5.group(8))

                best_mos = top_with_year(netflix, q5.group(4), find_yr)

                if best_mos is None:
                    print("Could not find a movie or show with that year.")
                    continue

                if q5.group(7).__contains__("last"):
                    response = f"The best {q5.group(4)} last year was {best_mos.get('title').title()}."
                elif q5.group(7).__contains__("this"):
                    response = f"The best {q5.group(4)} this year was {best_mos.get('title').title()}."
                elif q5.group(7).__contains__("ago"):
                    response = f"The best {q5.group(4)} {q5.group(8)} years ago was {best_mos.get('title').title()}."
                else:
                    response = f"The best {q5.group(4)} in {q5.group(8)} was {best_mos.get('title').title()}."

                print(response)
        elif q6 is not None:
            # print(q6.group(4), "|", q6.group(5), "|", q6.group(6), "|", q6.group(7), "|", q6.group(8))
            # , "|", q6.group(9), "|", q6.group(10), "|", q6.group(11), "|", q6.group(12), "|")
            if q6.group(5) is None:
                worst_mos = how_many_bottom_with_year(netflix, q6.group(4), 1)
                if len(worst_mos) <= 0:
                    print("Could not find a movie or show with that year.")
                    continue
                response = f"A {q6.group(4)} with the lowest rating was {worst_mos[0].get('title').title()}."
                print(response)
            else:
                yr = date.today().year
                if q6.group(7).__contains__("last"):
                    find_yr = yr - 1
                elif q6.group(7).__contains__("this"):
                    find_yr = yr
                elif q6.group(7).__contains__("ago"):
                    find_yr = yr - int(q6.group(8))
                else:
                    find_yr = int(q6.group(8))

                better_mos = how_many_bottom_with_year(netflix, q6.group(4), 1, find_yr)

                if len(better_mos) <= 0:
                    print("Could not find a movie or show with that year.")
                    continue

                if q6.group(7).__contains__("last"):
                    response = f"A {q6.group(4)} with the lowest rating last year was " \
                               f"{better_mos[0].get('title').title()}."
                elif q6.group(7).__contains__("this"):
                    response = f"A {q6.group(4)} with the lowest rating this year was " \
                               f"{better_mos[0].get('title').title()}."
                elif q6.group(7).__contains__("ago"):
                    response = f"A {q6.group(4)} with the lowest rating was {q6.group(8)} years ago was " \
                               f"{better_mos[0].get('title').title()}."
                else:
                    response = f"A {q6.group(4)} with the lowest rating was in {q6.group(8)} was " \
                               f"{better_mos[0].get('title').title()}."

                print(response)
        elif q7 is not None:
            # print(q7.group(1), "|", q7.group(2), "|", q7.group(5), "|", q7.group(6), "|", q7.group(7),
            # "|", q7.group(8), "|", q7.group(9))
            if not (q7.group(4).__contains__("movie") or q7.group(4).__contains__("show")):
                mos = duration(netflix, title=q7.group(3))
                if mos is None:
                    print("Could not find a movie or show.")
                    continue
                response = f"'{mos.get('title').title()}' is {mos.get('runtime')} minutes long."
                print(response)
            else:
                yr = date.today().year
                if q7.group(7).__contains__("last"):
                    find_yr = yr - 1
                elif q7.group(7).__contains__("this"):
                    find_yr = yr
                elif q7.group(7).__contains__("ago"):
                    find_yr = yr - int(q7.group(8))
                else:
                    find_yr = int(q7.group(8))

                if q7.group(2).__contains__("top"):
                    best_mos = top_with_year(netflix, q7.group(4), find_yr)
                    if best_mos is None:
                        print("Could not find a movie or show with that year.")
                        continue
                    response = f"'{best_mos.get('title').title()}' is the top {q7.group(4)} that year and is " \
                               f"{best_mos.get('runtime')} minutes long."
                else:
                    worst_mos = how_many_bottom_with_year(netflix, q7.group(4), 1, find_yr)
                    if len(worst_mos) <= 0:
                        print("Could not find a movie or show with that year.")
                        continue
                    response = f"'{worst_mos[0].get('title').title()}' is the bottom {q7.group(4)} that year and is " \
                               f"{worst_mos[0].get('runtime')} minutes long."
                print(response)
        elif q8 is not None:
            # print("|", q8.group(1), "|")
            mos = released(netflix, q8.group(1))
            if mos is None:
                print("Could not find a movie or show.")
                continue
            response = f"'{mos.get('title').title()}' was released in {mos.get('release_year')}."
            print(response)
        elif q9 is not None:
            # print("|", q9.group(1), "|")
            mos = released(netflix, q9.group(1))
            if mos is None:
                print("Could not find a movie or show.")
                continue
            response = f"'{mos.get('title').title()}' was released in {mos.get('release_year')}."
            print(response)
        elif q10 is not None:
            # print(q10.group(1), "|", q10.group(2), "|", q10.group(3), "|", q10.group(4), "|")
            if q10.group(2) is not None:
                if q10.group(2).__contains__("movie"):
                    medtype = "movie"
                elif q10.group(2).__contains__("show"):
                    medtype = "show"
                else:
                    medtype = None
            else:
                medtype = None
            mean = getMean(netflix, medtype, int(q10.group(4)))

            if mean == -1:
                print(f"Could not calculate score of {q10.group(4)}.")
                continue

            if medtype is not None:
                response = f"The mean imdb score of {medtype}s in the year {q10.group(4)} was {mean}"
            else:
                response = f"The mean imdb score of the year {q10.group(4)} was {mean}"
            print(response)
        else:
            should_q = input("I'm sorry I didn't understand that. Would you like to see the accepted questions? (y/n)")
            if should_q.strip().lower()[0] == "y":
                questions()


def main():
    client, collection = connectToMongo(False)
    chatbot(collection, False)


if __name__ == '__main__':
    main()
