import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

#load the env variables
load_dotenv()



def connect_to_database(get_engine_only=False):
    try:
        print(os.environ["DB_URL_WWPA"])
        print(os.environ["POSTGRES_SCHEMA_WWPA"])
        
        engine=create_engine(str(os.environ["DB_URL_WWPA"]),echo=False,connect_args={"options":"-csearch_path={}".format(os.environ["POSTGRES_SCHEMA_WWPA"])},pool_size=1000, max_overflow=0)

        # ,pool_size=20, max_overflow=0

        if get_engine_only:
            return engine
        
        session=sessionmaker(engine,expire_on_commit=False)
        print("Connection Established!")
        return session

    except Exception as e:
        raise ConnectionError("There is some error connecting to data base")


