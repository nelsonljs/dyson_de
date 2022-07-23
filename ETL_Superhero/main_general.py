import pandas as pd
import re, os
from pathlib import Path
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# mongo_user = os.getenv('MONGO_USERNAME')
# mongo_pass = os.getenv('MONGO_PASSWORD')
mongo_user = 'admin'
mongo_pass = 'admin123'

mongo_uri = f'mongodb://{mongo_user}:{mongo_pass}@mongo:27017'
client = MongoClient(mongo_uri)

def etl_superhero(myrow):
    '''
    Write the function as a mapping function to allow parallelization
    '''
    myrow['Character Name'], myrow['World'] = get_last_parentheses(myrow['Name'])   

def get_last_parentheses(s):
    '''
    Get last set of parenthesis in string
    '''
    try:
        world = re.findall('\((.*?)\)',s)[-1]
    except IndexError:
        world = ''

    char_name = s.replace(f'({world})','').strip()
    return char_name, world

def main_general():
    '''
    main script to ingest general data.
    '''
    ## Get metadata
    ingestion_date = datetime.today()
    source = 'general'

    metadata = {
        'ingestion_date': ingestion_date,
        'ingestion_source': source
    }

    with client:
        mydb = client['hello_world']
        coll = mydb['superheroes']

        mydf_name = Path('data','marvel_dc_characters.csv')
        mydf = pd.read_csv(mydf_name.absolute(), encoding="ISO-8859-1", chunksize=1000)

        for chunk in mydf:

            chunk['Character Name'], chunk['World'] = zip(*chunk['Name'].map(get_last_parentheses))
            mydocs = chunk.to_dict(orient='records')
            for mydoc in mydocs:
                mydoc['metadata'] = metadata
                coll.insert_one(mydoc)

            break

        for mydoc in coll.find({}).limit(5):
            print(mydoc)

if __name__=='__main__':
    main_general()