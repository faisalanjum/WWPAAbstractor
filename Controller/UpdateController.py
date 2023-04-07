import sys
import pathlib
import json
import pandas as pd
from sqlalchemy import insert

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1])) 

from backend.db.models import BarometerCoast, BarometerInland, BarometerWestern


from backend.db.dbconnect import connect_to_database

from DataRetreivalControllerWWPA import DataRetreivalControllerWWPA

from lumberdataabstract import abstract_barometer_data as barometer_fin
from barometer import abstract_data as barometer
from lumberdataabstract import abstract_data as lumber
from westernfactabstract import abstract_data as western
from common import fetch_files,insert_into_database,save_to_dir
import time


class UpdateController(DataRetreivalControllerWWPA):


    def __init__(self,lumber_track_path,western_lumber_fact_path,barometer_path) -> None:

   
   
        self.lumber_path=lumber_track_path
        self.western_path=western_lumber_fact_path
        self.barometer_path=barometer_path
        
        super().__init__()



    def update_western_lumber_facts(self):
        st=time.time()
        print("-----------------Western Lumber Updation Started----------------")
        
        pdfs=fetch_files(PDF_DIR=self.western_path)
        western(pdfs,save_in_file="average_price_white_woods",date_only=False)
        print("UpdationCompleted time taken to completely abstract data is {} secs".format(time.time()-st))
        


    def update_lumber_track(self):
        st=time.time()
        print("----------------- Lumber Track Updation Started---------------")
       
        pdfs=fetch_files(PDF_DIR=self.lumber_path) 
        lumber(pdfs,save_in_file="production_us",date_only=False)
        print("UpdationCompleted time taken to completely abstract data is {} secs".format(time.time()-st))

    
    
    def update_barometer(self,check_path):
        st=time.time()
        print("----------------- Barometer Track Updation Started----------------")
        pdfs=fetch_files(PDF_DIR=self.barometer_path)
        barometer_fin(pdfs)
        western_df, coast_df, inland_df= barometer(pdfs,check_path)

        #get data from database

        coastal_data=self.query_data(BarometerCoast)
        inland_data=self.query_data(BarometerInland)
        western_data=self.query_data(BarometerWestern)

        # create df 

        # if coastal_data:
        #     compare_cstl=pd.DataFrame.from_dict(coastal_data)
        #     coast_df = pd.concat([coast_df,compare_cstl]).drop_duplicates(keep=False)
      
        
        # if inland_data:
        #     compare_inland=pd.DataFrame.from_dict(inland_data)
        #     inland_df = pd.concat([inland_df,compare_inland]).drop_duplicates(keep=False)
        
        
        
        # if western_data:
        #     compare_western=pd.DataFrame.from_dict(western_data)
        #     western_df = pd.concat([western_df,compare_western]).drop_duplicates(keep=False)
       
        
        
        #get unique_data
        
        
        
        save_to_dir(coast_df,directory="DATA",sub_dir="Barometer",filename="barometer_coast.csv",mapper=BarometerCoast,process=True)
        save_to_dir(western_df,directory="DATA",sub_dir="Barometer",filename="barometer_western.csv",mapper=BarometerWestern,process=True)
        save_to_dir(inland_df,directory="DATA",sub_dir="Barometer",filename="barometer_island.csv",mapper=BarometerInland,process=True)
        
        print("UpdationCompleted time taken to completely abstract data is {} secs".format(time.time()-st))
        

