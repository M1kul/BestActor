# Part of an assignment to demonstrate several methods for determing the best
# actors. The code below fetches public IMDb datasets from
# https://www.imdb.com/interfaces/ and processes them into small subsets
# for reading into Power BI.

import pandas as pd
import requests
import os
import re

# For the analysis only a subset of actors are needed. The subset comes from
# another set of data provided by the assignment.

actors = pd.read_csv(r'C:\Users\cus64424\Dropbox\Notes\Personal\PowerBI\ActorsList.txt')
actors['actors_cleaned'] = actors['Actors'].str.lower()
actorlist = actors['actors_cleaned'].tolist()
print(actors.info())




# IMDb Datasets

## name.basics.tsv.gz – Contains the following information for names:
# nconst (string) - alphanumeric unique identifier of the name/person
# primaryName (string)– name by which the person is most often credited
# birthYear – in YYYY format
# deathYear – in YYYY format if applicable, else '\N'
# primaryProfession (array of strings)– the top-3 professions of the person
# knownForTitles (array of tconsts) – titles the person is known for

names = pd.read_csv(
    r"C:\Users\cus64424\Documents\IMDb\name.basics.tsv",
    sep='\t',
    usecols=[0, 1, 2, 3, 4],
    dtype={
        'nconst': object,
        'primaryName': object,
        'birthYear': 'Int64',
        'deathYear': 'Int64',
        'primaryProfession': object,
    },
    na_values="\\N",
    engine="c"
)

names['primaryName_clean'] = (
    names['primaryName']
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.lower()
        .str.replace('-', ' ')
)

nameset = (
    names[names['primaryName_clean'].isin(actorlist)]
    .sort_values(
        ['primaryName_clean', 'birthYear', 'nconst'],
        ascending=[True, True, True]
    )
    .drop_duplicates(subset=['primaryName_clean'])
    .drop(['primaryProfession'], axis=1)
    .reset_index(drop=True)
)
print(nameset.info())

# Search for specific actor:
search = nameset[nameset['primaryName'].str.contains('Vergara')]
print(search)




## title.basics.tsv.gz - Contains the following information for titles:
# tconst (string) - alphanumeric unique identifier of the title
# titleType (string) – the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
# primaryTitle (string) – the more popular title / the title used by the filmmakers on promotional materials at the point of release
# originalTitle (string) - original title, in the original language
# isAdult (boolean) - 0: non-adult title; 1: adult title
# startYear (YYYY) – represents the release year of a title. In the case of TV Series, it is the series start year
# endYear (YYYY) – TV Series end year. ‘\N’ for all other title types
# runtimeMinutes – primary runtime of the title, in minutes
# genres (string array) – includes up to three genres associated with the title

titles = pd.read_csv(
    r'C:\Users\cus64424\Documents\IMDb\title.basics.tsv',
    sep='\t',
    usecols=[0, 1, 2, 4, 5, 8],
    dtype={'tconst': object,
           'titleType': object,
           'primaryTitle': object,
           'isAdult': 'Int64',
           'startYear': 'Int64',
           'genres': object},
    na_values="\\N",
    engine="c")

# filter non-adult movies created between 1929 and 2021
titleset = (
    titles[(titles['startYear'].between(1929, 2021)) &
           (titles['titleType'] == 'movie') &
           (titles['isAdult'] == 0)]
    .drop(['titleType', 'isAdult'], axis=1)
)
print(titleset.info())




## title.principals.tsv.gz – Contains the principal cast/crew for titles
# tconst (string) - alphanumeric unique identifier of the title
# ordering (integer) – a number to uniquely identify rows for a given titleId
# nconst (string) - alphanumeric unique identifier of the name/person
# category (string) - the category of job that person was in
# job (string) - the specific job title if applicable, else '\N'
# characters (string) - the name of the character played if applicable, else '\N'

principals = pd.read_csv(
    r'C:\Users\cus64424\Documents\IMDb\title.principals.tsv',
    sep='\t',
    usecols=[0,2,3],
    dtype={'tconst': object,
           'nconst': object,
           'category': 'category'},
    na_values="\\N",
    engine="c")
print(principals.info())

principalset = (
    principals[(principals['category'].isin(['actor', 'actress'])) &
               (principals['tconst'].isin(titleset['tconst'])) &
               (principals['nconst'].isin(nameset['nconst']))]
    .drop('category', axis=1)
)
print(principalset.info())




## title.ratings.tsv.gz – Contains the IMDb rating and votes information for titles
# tconst (string) - alphanumeric unique identifier of the title
# averageRating – weighted average of all the individual user ratings
# numVotes - number of votes the title has received

ratings = pd.read_csv(
    r'C:\Users\cus64424\Documents\IMDb\title.ratings.tsv',
    sep='\t',
    dtype={'tconst': object,
           'averageRating': 'Float64',
           'numVotes': 'Int64'},
    na_values="\\N",
    engine="c")
print(ratings.info())





# filter each set by the other so only needed data is kept.
titleset = titleset[titleset['tconst'].isin(principalset['tconst'])]
ratingset = ratings[ratings['tconst'].isin(principalset['tconst'])]

print(nameset.info())
print(principalset.info())
print(titleset.info())
print(ratingset.info())

# And output to csv's
nameset.to_csv('IMDb_Actors_Subset.csv', index=False)
principalset.to_csv('IMDb_Cast_Subset.csv', index=False)
titleset.to_csv('IMDb_Movie_Subset.csv', index=False)
ratingset.to_csv('IMDb_Ratings_Subset.csv', index=False)




# Get Academy Award winning and nominated actors from Wikipedia
academy_awards_actors_url = 'https://en.wikipedia.org/wiki/List_of_actors_with_two_or_more_Academy_Award_nominations_in_acting_categories'
r = requests.get(academy_awards_actors_url)
df = pd.read_html(r.text)

def parse_academy_award_actors_wiki_tables(dataframe, actor_column, film_list_column, award_status):
    """ Process the queried tables from Wikipedia.

        Parameters
        ----------
        dataframe : Pandas dataframe
            dataframe containing the Wiki table data from read_html
        actor_column : str
            The header for the column containing the actor's name.
        film_list_column : str
            The header for the column containing the film's name.
        award_status : str
            'Winner' or 'Nominee', is this table of winners or nominees?
    """
    def clean_actor_names(name):
        """ Parse actors names from suffixes."""
        match = re.search('^(.*?)\[', name)
        if match:
            return match.group(1)
        return name
    
    step1 = (
        pd.concat([dataframe[[actor_column]], dataframe[film_list_column].str.split('\),', regex=True, expand=True)], axis=1)
        .melt(id_vars=[actor_column], var_name='Win No.', value_name='Film')
        .sort_values(actor_column)
        .replace('—', None)
        .dropna()
    )
    step2 = (
        step1
        .assign(
            actorName=step1[actor_column].apply(lambda x: clean_actor_names(x)),
            movieTitle=step1['Film'].str.extract(r'^(.*?)\s\(', expand=False).str.strip().str.title(),
            createdYear=step1['Film'].str.extract(r'\((\d{4})', expand=False),
            awardStatus=award_status
        )
        .drop([actor_column, 'Win No.', 'Film'], axis=1)
    )
    return step2


academy_awards_actors = (
    pd.concat([
        parse_academy_award_actors_wiki_tables(df[0], 'Actor/Actress', 'Winning film(s)', 'Winner'),
        parse_academy_award_actors_wiki_tables(df[2], 'Actress', 'Nominated films', 'Nominee'),
        parse_academy_award_actors_wiki_tables(df[1], 'Actor', 'Nominated films', 'Nominee')
    ])
    .sort_values(['actorName', 'movieTitle', 'awardStatus'], ascending=[True, True, False])
    .drop_duplicates(subset=['actorName', 'movieTitle'])
)

# Output
academy_awards_actors.to_csv('Academy_Award_Actors.csv', index=False)