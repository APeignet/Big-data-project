from elasticsearch import Elasticsearch
import csv
import datetime

PATH_TO_CHANGE = '/Users/angel/airflow'


def indexingData(**kwargs):
    es = Elasticsearch("http://localhost:9200")
    es.info()

    index_name = 'movies_data_records'
    es.indices.delete(index=index_name, ignore=[400, 404])
    es.indices.create(index=index_name, ignore=400)
    es.indices.put_mapping(
        index=index_name,
        ignore=400,
        body={

            "properties": {

                "budget": {"type": "long"},
                "id": {"type": "keyword"},
                "original_language": {"type": "keyword"},
                "original_title": {"type": "keyword"},
                "revenue": {"type": "long"},
                "runtime": {"type": "long"},
                "title": {"type": "keyword"},
                "release_date": {"type": "date"},
                "popularity": {"type": "float"},
                "vote_average": {"type": "float"},
                "vote_count": {"type": "integer"},
                "__index_level_0__": {"type": "integer"}

            }

        }
    )


    pathCSV = PATH_TO_CHANGE + "/DataLake/usage/my_usage2/DataEntity1/" + str(
        datetime.date.today()) + "/moviesData.csv"

    with open(pathCSV, 'r') as file:
        reader = csv.reader(file)
        for i, row in enumerate(reader):
            if i > 0:
                es.create(index=index_name, id=i, body={'budget': row[0], 'id': row[1], 'original_language': row[2],
                                                        'original_title': row[3], 'revenue': row[4], 'runtime': row[5],
                                                        'title': row[6], 'release_date': row[7],
                                                        'popularity': row[8],
                                                        'vote_average': row[9], 'vote_count': row[10]})

    file.close()
