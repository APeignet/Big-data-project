import requests
import pandas as pd
import datetime
import os

PATH_TO_CHANGE = '/Users/angel/airflow'

def flatten_json(y):
    out = {}

    def flatten(x, name=''):

        if type(x) is dict:

            for a in x:
                flatten(x[a], name + a + '_')

        elif type(x) is list:

            i = 0

            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def recupDataApiJSON(**kwargs):
    for i in range(1, 51):
        url = 'https://api.themoviedb.org/3/discover/movie?api_key=6c0f7e7adc6f11336966adba7f31624e&sort_by=popularity.desc&include_video=false&page=' + str(
            i)
        req = requests.get(url).json()
        if i == 1:
            df = pd.json_normalize(req['results'])
        else:
            dfBis = pd.json_normalize(req['results'])
            df = pd.concat([df, dfBis])
            del dfBis

    df = df.reset_index()
    listId = df['id']

    for i in range(len(listId) - 1):
        url2 = 'https://api.themoviedb.org/3/movie/' + str(listId[i]) + '?api_key=6c0f7e7adc6f11336966adba7f31624e'
        req2 = requests.get(url2).json()
        req2_flatten = [flatten_json(req2)]
        if i == 0:
            df2 = pd.DataFrame.from_dict(req2_flatten)
        else:
            df3 = pd.DataFrame.from_dict(req2_flatten)
            df2 = pd.concat([df2, df3])
            del df3
        del req2_flatten

    df2 = df2.reset_index()
    dfFinal = df2[['id', 'original_title', 'title', 'original_language', 'budget', 'revenue', 'runtime']]

    js = dfFinal.to_json(orient='records')

    pathDate = PATH_TO_CHANGE+'/DataLake/raw/source2/DataEntity1/'+str(datetime.date.today())
    if not os.path.exists(pathDate):
        os.makedirs(pathDate)

    pathJson = PATH_TO_CHANGE+"/DataLake/raw/source2/DataEntity1/"+str(datetime.date.today())+"/moviesAPI.json"
    file1 = open(pathJson, "w")
    file1.writelines(js)
    file1.close()

    print('Data récupérées')