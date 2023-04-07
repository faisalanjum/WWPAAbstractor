import sys,pathlib

from WesternWoodPDFs.westernpdfabstractor.backend.db.models import BarometerCoast, BarometerInland, BarometerWestern
sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))
print(str(pathlib.Path(__file__).resolve().parents[2]))
from backend.db.models import *
from backend.db.dbconnect import connect_to_database
import pandas as pd
import time
#wrapper function to add data iny
def insert_into_database(mapper, data):

    db_engine = connect_to_database()
    ssn = db_engine()
    try:
        ssn.bulk_insert_mappings(mapper,data)
        ssn.commit()
        print("records added")

    except Exception as e:
      
        ssn.rollback()
        try:
            ssn.bulk_update_mappings(mapper, data)
            ssn.commit()
            print("records updated")
        except Exception as e:
            print("there is some error in adding data to db \n {}".format(e))

    finally:
        ssn.close()

def populate(historical=True):
    
    print("started populating barometer_western data")
    st=time.time()
    barometer_df=pd.read_csv("barometer_western.csv")
    df=barometer_df.drop_duplicates(subset=['File'],keep='last')

    insert_into_database(BarometerWestern,df.to_dict(orient="records"))
    print("barometer westen data populated in {} seconds".format(time.time()-st))
    
    print("started populating barometer inlands data")
    st=time.time()
    barometer_df=pd.read_csv("barometer_inlands.csv")
    df=barometer_df.drop_duplicates(subset=['File'],keep='last')
    insert_into_database(BarometerInland,df.to_dict(orient="records"))
    print("barometer inland data populated in {} seconds".format(time.time()-st))
    
    print("started populating barometer coastal data")
    st=time.time()
    barometer_df=pd.read_csv("barometer_coast.csv")
    df=barometer_df.drop_duplicates(subset=['File'],keep='last')
    insert_into_database(BarometerCoast,df.to_dict(orient="records"))
    print("barometer coast data populated in {} seconds".format(time.time()-st))



