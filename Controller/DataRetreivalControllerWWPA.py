import sys
import pathlib
import json
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1])) 
from backend.db.dbconnect import connect_to_database as db_cot
from backend.db.models import *


class DataRetreivalControllerWWPA:

    def __init__(self):

        self.session=db_cot()
    
    def close_session(self):
        self.session.close()
    


    
    def query_data(self,mapper,parameter_val=None,parameter="id"):

            session=self.session()
            if type(parameter_val) == str:
                q_res=session.query(mapper).filter( getattr(mapper,parameter) ==  parameter_val).all()
                res=[q.toDict() for q in q_res]
                return res


            elif type(parameter_val) == list:
                prm=getattr(mapper,parameter)
                q_res=session.query(mapper).filter( prm.in_(parameter_val)).all()
                res=[q.toDict() for q in q_res]
                return res

              


            elif parameter_val == None:
               
                q_res=session.query(mapper).all()
                res=[q.toDict() for q in q_res]
                return res

                
            
            else:
                print("enter vsalid par_value str and list is valid type ")


    
    
    def get_latest_series(self,mapper,parameter_val=None,parameter="id"):

            session=self.session()
            if type(parameter_val) == str:
                q_res=session.query(mapper).filter( getattr(mapper,parameter) ==  parameter_val).order_by(mapper.date.desc()).first()
                res=q_res.toDict()
                return res


            elif type(parameter_val) == list:
                prm=getattr(mapper,parameter)
                q_res=session.query(mapper).filter( prm.in_(parameter_val)).order_by(mapper.date.desc()).first()
                res=q_res.toDict()
                return res


              


            elif parameter_val == None:
               
                q_res=session.query(mapper).order_by(mapper.date.desc()).first()
                res=q_res.toDict()
                return res

                
            
            else:
                print("enter valid par_value str and list is valid type ")

    


#test
# obj=DataRetreivalController()

# data=obj.query_data(NorthAmericanInventory)
# print(data)