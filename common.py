
import os
from pathlib import Path
import pandas as pd
from backend.db.dbconnect import connect_to_database
PDFS=[]


#ignore list

IGNORE_LIST=["LT1601.pdf",
"LT1602.pdf",
"LT1603.pdf",
"LT1604.pdf"
"LT1605 revised v2.pdf",
"LT1605 revised (2).pdf",
"LT1605.pdf",
"LT1702.pdf",
"LT2001Prelim.pdf",
"LT2002Prelim.pdf",
"LT2003Prelim.pdf",
"LT2004Prelim.pdf",
]

# function checks if recoed and file exists

def check_if_exists(filepath,checkparm,relative=True):
       
        if relative:
            filepath = (Path(__file__).resolve().parent).joinpath(filepath)
        check=os.path.exists(str(filepath))
        if check:
            df=pd.read_csv(str(filepath))
       
            files=df["File"].values
        

        
            if str(checkparm) in files:

                return True

            else :
                return False

        else:
            return False



def fetch_files(dir_name=None,sub_dir_name=None,PDF_DIR=None):
    PDFS.clear()
    fetch_files_from_directory(dir_name,sub_dir_name,PDF_DIR)
    return PDFS




def fetch_files_from_directory(dir_name="docs",sub_dir_name=None,PDF_DIR=None):
    if not PDF_DIR: 
        PDF_DIR = (Path(__file__).resolve().parent).joinpath(dir_name)
    if sub_dir_name:
        PDF_DIR = PDF_DIR.joinpath(sub_dir_name)
    files= [f for f in Path(PDF_DIR).iterdir() if f.is_file() and str(f).endswith('.pdf')]
    files= [PDFS.append(f) for f in files]
   
    sub_dirs= [d for d in Path(PDF_DIR).iterdir() if d.is_dir()]
    
    if sub_dirs:
        [fetch_files_from_directory(PDF_DIR=d) for d in Path(PDF_DIR).iterdir() if d.is_dir()]



def table_to_csv(tables):
    for i,table in enumerate(tables):
        name="table_"+str(i)+".csv"
        table.to_csv(name)






def save_to_dir(df,directory,sub_dir,filename,mapper=None,process=True):

    if process == True:

        try:
            df['Timestamp'] =  pd.to_datetime(df['date'],format="%b-%y").dt.normalize()
        
        except ValueError:
             df['Timestamp'] =  pd.to_datetime(df['date'],format="%Y-%m-%d").dt.normalize()

        
        df['Year'] = pd.DatetimeIndex(df['Timestamp']).year
        df["Month"]=pd.DatetimeIndex(df['Timestamp']).month
        df["Day"]=pd.DatetimeIndex(df['Timestamp']).day

    path = (Path(__file__).resolve().parent).joinpath(directory).joinpath(sub_dir)
        # check dir or create it 
    
    if not os.path.exists(path):
        os.makedirs(path)
    
    filepath=os.path.join(path,filename)
    # create or append data to it

    
    if not os.path.isfile(str(filepath)):
        df.to_csv(str(filepath), header='column_names')
    else: 
        df.to_csv(str(filepath), mode='a', header=False)
    
    if mapper:
        insert_into_database(mapper,df.to_dict(orient="records"))

    


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
            raise e
            print("there is some error in adding data to db \n {}".format(e))

    finally:
        ssn.close()