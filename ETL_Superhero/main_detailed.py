import pandas as pd
import numpy as np
import logging
import os
from pathlib import Path
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

mongo_user = os.getenv('MONGO_USERNAME')
mongo_pass = os.getenv('MONGO_PASSWORD')
# mongo_user = 'admin'
# mongo_pass = 'admin123'

class IngestDetailed:
    '''
    Ingestion for detailed data. Wrap in class since we are ingesting 3 csvs at once.
    '''
    error_stat_update = []

    def __init__(self):
        ## Get metadata
        ingestion_date = datetime.today()
        source = 'detailed'

        self.ingestion_id = f'{source}_{ingestion_date.strftime("%Y%m%d")}'

        self.metadata = {
            'ingestion_date': ingestion_date,
            'ingestion_source': source,
            'ingestion_id': self.ingestion_id
        }

        self.mongo_uri = f'mongodb://{mongo_user}:{mongo_pass}@mongo:27017'

    @staticmethod
    def _get_universe(s):
        '''
        map_function to get universe.
        '''
        s = str(s)
        if 'Marvel' in s:
            return 'Marvel'
        elif 'DC' in s:
            return 'DC'
        else:
            return ''

    def ingest_main_info(self):
        '''
        Ingest main info.
        '''

        mydf_name = Path('data','smaller_set','marvel_characters_info.csv')
        ## Handle na values, raw data reads as -
        mydf = pd.read_csv(mydf_name.absolute(), na_values=['-'], chunksize=1000)

        for chunk in mydf:
        ## Handle invalid height/weight values.
            chunk.drop(columns=['ID'], inplace=True)
            chunk.loc[chunk['Height']==-99,'Height'] = np.NaN
            chunk.loc[chunk['Weight']==-99,'Weight'] = np.NaN

            ## Some basic cleanup
            capitalize_cols = ['Alignment','Gender','EyeColor','Race','HairColor','SkinColor']
            for col in capitalize_cols:
                chunk[col] = chunk[col].str.capitalize()

            chunk['Universe'] = chunk['Publisher'].map(self._get_universe)

            with MongoClient(self.mongo_uri) as c:
                mydb = c['hello_world']
                coll = mydb['superheroes']

                mydocs = chunk.to_dict(orient='records')
                for mydoc in mydocs:
                    mydoc['metadata'] = self.metadata
                    coll.insert_one(mydoc)
    
    def ingest_stat_info(self):
        '''
        Ingest Stat info after main, we assume the name is aligned, since character stats do not have any other identifier.
        '''

        mydf_name = Path('data','smaller_set','charcters_stats.csv')
        ## Handle na values, raw data reads as -
        mydf = pd.read_csv(mydf_name.absolute(), na_values=['-'], chunksize=1000)
        with MongoClient(self.mongo_uri) as c:
            mydb = c['hello_world']
            coll = mydb['superheroes']

            for chunk in mydf:
                mydocs = chunk.to_dict(orient='records')

                for mydoc in mydocs:
                    try:
                        del mydoc['Alignment']
                    except:
                        pass

                    query = {
                        'Name': mydoc['Name'],
                        'metadata.ingestion_id': self.ingestion_id
                    }

                    stats = {k:v for k,v in mydoc.items() if k !='Name'}

                    update = {
                        "$set": {
                            "Character Stats": stats
                        }
                    }

                    updated = coll.update_many(query, update)

                    if updated.matched_count < 1:
                        logging.error('Unmatched name.')
                        self.error_stat_update.append(mydoc)

    def ingest_power_matrix_info(self):
        '''
        Ingest Power Matrix info after main, we assume the name is aligned, since character stats do not have any other identifier.
        '''

        mydf_name = Path('data','smaller_set','superheroes_power_matrix.csv')
        ## Handle na values, raw data reads as -
        mydf = pd.read_csv(mydf_name.absolute(), na_values=['-'], chunksize=1000)

        # TODO: Verification of data?

        with MongoClient(self.mongo_uri) as c:
            mydb = c['hello_world']
            coll = mydb['superheroes']

            for chunk in mydf:
                mydocs = chunk.to_dict(orient='records')

                for mydoc in mydocs:

                    query = {
                        'Name': mydoc['Name'],
                        'metadata.ingestion_id': self.ingestion_id
                    }

                    power_matrix = {k:v for k,v in mydoc.items() if k !='Name'}

                    update = {
                        "$set": {
                            "Character Stats": power_matrix
                        }
                    }

                    updated = coll.update_many(query, update)

                    if updated.matched_count < 1:
                        logging.error('Unmatched name.')
                        self.error_stat_update.append(mydoc)

    def main(self):
        '''
        driver
        '''
        self.ingest_main_info()
        self.ingest_stat_info()
        self.ingest_power_matrix_info()

    def query_one(self):
        '''
        sample query to check.
        '''

        with MongoClient(self.mongo_uri) as c:
            mydb = c['hello_world']
            coll = mydb['superheroes']

            # for mydoc in coll.find({"Name": 'A-Bomb'}).limit(5):
            for mydoc in coll.find({'metadata.ingestion_id': self.ingestion_id}).limit(5):
                print(mydoc)

if __name__=='__main__':
    a = IngestDetailed()
    a.main()
    # a.query_one()
