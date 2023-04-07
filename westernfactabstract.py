
import os
from pathlib import Path
import pandas as pd
import time

from tabula import read_pdf
from tabulate import tabulate
from backend.db.models import *
from common import fetch_files,save_to_dir,table_to_csv,check_if_exists
from lumberdataabstract import format_data
# wrapper funtion to abstract data

def abstract_data(pdf_files:list,save_in_file="pppc_western",date_only=False):


    for i,f in enumerate(pdf_files):
        
        # splits the file path
        f_name=str(f)
        f_path="DATA/LumberFacts/"+save_in_file+".csv"

        #check if record from file exists
         
        check_existance=check_if_exists(f_path,f_name)
        
        #if recoed not exists calls the abstract function to process and put data in the csv

        if not check_existance :
            #abstract data from file
            production_data = read_pdf(str(f),pages="1-2",area=(17, 0, 24, 100),stream=True,relative_area=True)
            shipment_data = read_pdf(str(f),pages="1-2",area=(24, 0, 29, 100),stream=True,relative_area=True)
            orders_data = read_pdf(str(f),pages="1-2",area=(30, 0, 36, 100),stream=True,relative_area=True)
            unfilled_orders_data = read_pdf(str(f),pages="1-2",area=(37, 0, 44, 40),stream=True,relative_area=True)
            inventories_data = read_pdf(str(f),pages="1-2",area=(45, 0, 52, 40),stream=True,relative_area=True)
            pppc_data = read_pdf(str(f),pages="1-2",area=(54, 0, 60, 62),stream=True,relative_area=True)
            shipment_costal_data = read_pdf(str(f),pages="1",area=[(77, 0, 85, 100)],stream=True,relative_area=True)
            shipment_inland_data = read_pdf(str(f),pages="1",area=[(86, 0, 92, 100)],stream=True,relative_area=True)
            average_price_coast = read_pdf(str(f),pages="2",area=[(10, 0,17 , 100)],stream=True,relative_area=True)
            average_price_island = read_pdf(str(f),pages="2",area=[(18, 0,26 , 100)],stream=True,relative_area=True)
            average_price_costal_doglus = read_pdf(str(f),pages="2",area=[(32, 0,36 , 100)],stream=True,relative_area=True)
            average_price_costal_ham_fir = read_pdf(str(f),pages="2",area=[(37, 0,40,100)],stream=True,relative_area=True)
            average_price_island_douglas_fir = read_pdf(str(f),pages="2",area=[(41, 0,44,100)],stream=True,relative_area=True)
            average_price_white_wood = read_pdf(str(f),pages="2",area=[(46, 0,48,100)],stream=True,relative_area=True)
            average_price_ponderaso_pine = read_pdf(str(f),pages="2",area=[(50, 0,56,100)],stream=True,relative_area=True)
           
           
            #process production data
            table_to_csv(average_price_ponderaso_pine)
            date=abstract_production(production_data,f,just_date=date_only)
            abstract_orders(orders_data,f,date_index=date)
            abstract_unfilled_orders(unfilled_orders_data,f,date_index=date)
            abstract_inventories(inventories_data,f,date_index=date)
            abstract_pppc(pppc_data,f,date_index=date)
            abstract_shipment(shipment_data,f,date_index=date)

            abstract_shipmwnt_to_costal(shipment_costal_data,f,date_index=date)
            abstract_shipmwnt_to_island(shipment_inland_data,f,date_index=date)
            abstract_average_price_coast_region(average_price_coast,f,date_index=date)
            abstract_average_price_island_region(average_price_island,f,date_index=date)
            abstract_average_price_costal_doglus(average_price_costal_doglus,f,date_index=date)
            abstract_average_price_costal_ham_fir(average_price_costal_ham_fir,f,date_index=date)
            abstract_average_price_island_dauglas_fir(average_price_island_douglas_fir,f,date_index=date)
            abstract_average_price_ponderaso_pine(average_price_ponderaso_pine,f,date_index=date)
            abstract_average_price_white_woods(average_price_white_wood,f,date_index=date)





        
        else:
            print("Record already updated")


      



#function below abstracts the  production data from the abstracted tables

def abstract_production(tables,f,save_in_file='production_us',just_date=False):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name=str(f)
        
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
    
        date=process_production(tables,pathname=f_name,filename=save_in_file,date_only=just_date)
    except IndexError as e:
     
        try:
                date=process_production(tables,pathname=f_name,table_index=1,filename=save_in_file,date_only=just_date)
        except IndexError as e:
            raise e
            # try:
            # #     tables=read_pdf(str(f),pages="1-3",area=(7, 0, 14, 100),stream=True,relative_area=True)
            #     date=process_production(tables,pathname=f_name,table_index=1,filename=save_in_file,date_only=just_date)
            # except IndexError as e:
            #     pass
    return date













#process table one
def process_production(tables,pathname,table_index=0,filename="production_western",date_only=False):
    """
    Description
    -----------

    function processes the production data 


    Parameters
    -----------

    tables:list() of table dfs
    pathname:list splitted file path
    table_index index of table to abstract data from
    filename:(str) name of file to save the data in
    
    
    Returns:
    datetime
    
    """



    table=tables[table_index]

    table.rename( columns={'Production':'date'}, inplace=True )
    
    
    prod_df=table.iloc[:,[0,5]]
    colmns=prod_df.columns.values
    date=colmns[1]

    if not date or "-"  not in date:
        raise IndexError
    
    prod_df=prod_df.transpose()
    prod_df.reset_index(inplace=True)
    columns=prod_df.iloc[0].values
    

    if  "date" not in columns :
        
        raise IndexError("THERE IS SOME ERROR WITH VALUES")

    
    if date_only:
        return date

    prod_df.columns=prod_df.iloc[0]
    prod_df=prod_df[1:]
    rename_dict={"Cal RW":"Cal_RW",'Western Total':'Western_Total'}
    prod_df=format_data(prod_df,prod_df.columns.tolist())
    prod_df.rename(columns=rename_dict,inplace=True)
    
    prod_df["File"]=pathname
    save_to_dir(prod_df,"DATA","LumberFacts",filename+".csv",ProductionWestern)
    return date





#function below abstracts the  shipment data from the abstracted tables

def abstract_shipment(tables,f,save_in_file="shipment_western",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name=str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        date=process_shipment(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
    except IndexError as e:
        try:
                date=process_shipment(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
        except IndexError as e:
            raise e





#process table one
def process_shipment(tables,pathname,date_index,table_index=0,filename="shipment_western"):
    """
    Description
    -----------

    function processes the us shipment data 


    Parameters
    -----------

    tables:list() of table dfs
    pathname:list splitted file path
    date:(str) date abstracted from production
    table_index index of table to abstract data from
    filename:(str) name of file to save the data in
    
    
    Returns:
    datetime
    
    """
    
    table=tables[table_index]


    if table.shape[1] in [12,13,14,15]:

        

        if table.shape[0]==4:
            table.columns=table.iloc[0,:].values
            table=table.iloc[1:,1:]
            prod_df=table.iloc[:,[0,5]]



        else:
            if table.shape[1] == 13:
                prod_df=table.iloc[:,[0,5]]
            else:
                prod_df=table.iloc[:,[1,5]]
            colmns=prod_df.columns.values
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)
        columns=prod_df.iloc[0].values
     
        if "Cal. Coast" in columns:
            check =  all(item in columns for item in ['Coast','Inland','Cal. Coast','Western Total'])
            rename_dict={"Cal. Coast":"Cal_Coast",'Western Total':'Western_Total'}


        else:
            check =  all(item in columns for item in ['Coast' ,'Inland' ,'Cal RW' ,'Western Total'])
            rename_dict={"Cal RW":"Cal_RW",'Western Total':'Western_Total'}


        
        if  not check:
            raise IndexError
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
        prod_df=format_data(prod_df,prod_df.columns.tolist())
        prod_df.rename(columns=rename_dict,inplace=True)


    
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","LumberFacts",filename+".csv",ShipmentWestern)

    else:
        raise IndexError







#function below abstracts the  shipment data from the abstracted tables

def abstract_orders(tables,f,save_in_file="orders_western",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name=str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_orders(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
    except IndexError as e:
        try:
                process_orders(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
        except IndexError as e:
          
            tables=read_pdf(str(f),pages="2",area=(31, 0, 40, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_orders(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
              
           





#process table one
def process_orders(tables,pathname,date_index,table_index=0,filename="order_western"):
    """
    Description
    -----------

    function processes the us shipment data 


    Parameters
    -----------

    tables:list() of table dfs
    pathname:list splitted file path
    date:(str) date abstracted from production
    table_index index of table to abstract data from
    filename:(str) name of file to save the data in
    
    
    Returns:
    datetime
    
    """
    
    table=tables[table_index]
  
    if table.shape[1] in [11,12,13,14,15] :

        table=table.dropna(thresh=table.shape[1]-4, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        prod_df=table.iloc[:,[0,5]]
        colmns=prod_df.columns.values
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)

        columns=prod_df.iloc[0].values
        

        if 'New Orders' in columns: 
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values
    

        if "Cal. Coast" in columns:
            check =  all(item in columns for item in ['Coast','Inland','Cal. Coast','Western Total'])
            rename_dict={"Cal. Coast":"Cal_Coast",'Western Total':'Western_Total'}


        else:
            check =  all(item in columns for item in ['Coast' ,'Inland' ,'Cal RW' ,'Western Total'])
            rename_dict={"Cal RW":"Cal_RW",'Western Total':'Western_Total'}
   
        
        if  not check:
            raise IndexError
        
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
        prod_df=format_data(prod_df,prod_df.columns.tolist())
        prod_df.rename(columns=rename_dict,inplace=True)
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","LumberFacts",filename+".csv",OrdersWestern)

    else:
        raise IndexError




def abstract_unfilled_orders(tables,f,save_in_file="unfilled_orders_western",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name=str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_unfilled_orders(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
    except IndexError as e:
        try:
                process_unfilled_orders(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
        except IndexError as e:
            raise e
          
         
           





#process table one
def process_unfilled_orders(tables,pathname,date_index,table_index=0,filename="unfilled_order_western"):
    """
    Description
    -----------

    function processes the us shipment data 


    Parameters
    -----------

    tables:list() of table dfs
    pathname:list splitted file path
    date:(str) date abstracted from production
    table_index index of table to abstract data from
    filename:(str) name of file to save the data in
    
    
    Returns:
    datetime
    
    """
    
    table=tables[table_index]

  
    if table.shape[1] in [4,5,6,7] :

        table=table.dropna(thresh=table.shape[1]-4, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        prod_df=table.iloc[:,[0,2]]
        colmns=prod_df.columns.values
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)

        columns=prod_df.iloc[0].values


        if 'Unfilled Orders' in columns: 
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values
    
        if "Cal. Coast" in columns:
            check =  all(item in columns for item in ['Coast','Inland','Cal. Coast','Western Total'])
            rename_dict={"Cal. Coast":"Cal_Coast",'Western Total':'Western_Total'}


        else:
            check =  all(item in columns for item in ['Coast' ,'Inland' ,'Cal RW' ,'Western Total'])
            rename_dict={"Cal RW":"Cal_RW",'Western Total':'Western_Total'}

        
        if  not check:
            raise IndexError
        
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
    

        if "Inventories" in prod_df.columns.tolist():
            prod_df=prod_df.iloc[:,1:]
        prod_df=format_data(prod_df,prod_df.columns.tolist())
        prod_df.rename(columns=rename_dict,inplace=True)
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","LumberFacts",filename+".csv",UnfilledOrderWestern)

    else:
        raise IndexError



def abstract_inventories(tables,f,save_in_file="inventories_western",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
    
    f_name=str(f)
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_inventories(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
    except IndexError as e:
        try:
                process_inventories(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
        except IndexError as e:
            raise e
          
         
           





#process table one
def process_inventories(tables,pathname,date_index,table_index=0,filename="inventories_western"):
    """
    Description
    -----------

    function processes the us shipment data 


    Parameters
    -----------

    tables:list() of table dfs
    pathname:list splitted file path
    date:(str) date abstracted from production
    table_index index of table to abstract data from
    filename:(str) name of file to save the data in
    
    
    Returns:
    datetime
    
    """
    
    table=tables[table_index]

  
    if table.shape[1] in [4,5,6,7] :

        table=table.dropna(thresh=table.shape[1]-4, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        prod_df=table.iloc[:,[0,2]]
        colmns=prod_df.columns.values
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)

        columns=prod_df.iloc[0].values
        

        if 'Inventories' in columns: 
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values
    
        if "Cal. Coast" in columns:
            check =  all(item in columns for item in ['Coast','Inland','Cal. Coast','Western Total'])
            rename_dict={"Cal. Coast":"Cal_Coast",'Western Total':'Western_Total'}


        else:
            check =  all(item in columns for item in ['Coast' ,'Inland' ,'Cal RW' ,'Western Total'])
            rename_dict={"Cal RW":"Cal_RW",'Western Total':'Western_Total'}

        
        if  not check:
            raise IndexError
        
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
        prod_df=format_data(prod_df,prod_df.columns.tolist())
        prod_df.rename(columns=rename_dict,inplace=True)
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","LumberFacts",filename+".csv",InventoryWestern)

    else:
        raise IndexError




def abstract_pppc(tables,f,save_in_file="pppc_western",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name=str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_pppc(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
    except IndexError as e:
        try:
                process_pppc(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
        except IndexError as e:
            try:
                tables = read_pdf(str(f),pages="1-2",area=(57, 0, 63, 55),stream=True,relative_area=True)
                table_to_csv(tables)
                process_pppc(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
            except IndexError as e:
                try:
                    process_pppc(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)

                except IndexError as e:
                    try:
                        tables = read_pdf(str(f),pages="1-2",area=(55, 0, 62, 55),stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_pppc(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)

                    except IndexError as e:
                        process_pppc(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)


            

         
                
        

          
         
           





#process table one
def process_pppc(tables,pathname,date_index,table_index=0,filename="pppc_western"):
    """
    Description
    -----------

    function processes the us shipment data 


    Parameters
    -----------

    tables:list() of table dfs
    pathname:list splitted file path
    date:(str) date abstracted from production
    table_index index of table to abstract data from
    filename:(str) name of file to save the data in
    
    
    Returns:
    datetime
    
    """
    
    table=tables[table_index]

  
    if table.shape[1] in [7,8] :

        table=table.dropna(thresh=table.shape[1]-4, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        prod_df=table.iloc[:,[0,4]]
        colmns=prod_df.columns.values
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)

        columns=prod_df.iloc[0].values
        if 'Unnamed: 0' in columns: 
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values
    
        if "Cal. Coast" in columns:
            check =  all(item in columns for item in ['Coast','Inland','Cal. Coast','Western Total'])
            rename_dict={"Cal. Coast":"Cal_Coast",'Western Total':'Western_Total'}


        else:
            check =  all(item in columns for item in ['Coast' ,'Inland' ,'Cal RW' ,'Western Total'])
            rename_dict={"Cal RW":"Cal_RW",'Western Total':'Western_Total'}

        
        
        if  not check:
            raise IndexError
        
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
        prod_df=format_data(prod_df,prod_df.columns.tolist())
        prod_df.rename(columns=rename_dict,inplace=True)
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","LumberFacts",filename+".csv",PPPCWestern)

    else:
        raise IndexError







def abstract_shipmwnt_to_costal(tables,f,save_in_file="shipment_costal",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name=str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_shipment_to_costal(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
    except IndexError as e:
       
            try:
                tables = read_pdf(str(f),pages="2",area=(81, 0, 89, 100),stream=True,relative_area=True)
                table_to_csv(tables)
                process_shipment_to_costal(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
            except IndexError as e:
                try:
                    tables = read_pdf(str(f),pages="1-2",area=(80, 0, 90, 100),stream=True,relative_area=True)
                  
                    table_to_csv(tables)
                    process_shipment_to_costal(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
         
                except IndexError as e:
                    raise e

            

         
                
        

          
         
           





#process table one
def process_shipment_to_costal(tables,pathname,date_index,table_index=0,filename="shipment_costal"):
    """
    Description
    -----------

    function processes the western costal shipment data 


    Parameters
    -----------

    tables:list() of table dfs
    pathname:list splitted file path
    date:(str) date abstracted from production
    table_index index of table to abstract data from
    filename:(str) name of file to save the data in
    
    
    Returns:
    datetime
    
    """
    
    table=tables[table_index]
   
  
    if table.shape[1] in [10,11] :

        table=table.dropna(thresh=table.shape[1]-4, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        prod_df=table.iloc[:,[0,5]]
        colmns=prod_df.columns.values
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)

        columns=prod_df.iloc[0].values
       

        if 'Unnamed: 0' in columns: 
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values
    

        check =  all(item in columns for item in ['Northeast', 'Midwest', 'South', 'West', 'Total'])


           
        
        if  not check:
            raise IndexError
        
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
        prod_df=format_data(prod_df,prod_df.columns.tolist())
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","LumberFacts",filename+".csv",ShipmentCoastal)

    else:
        raise IndexError





def abstract_shipmwnt_to_island(tables,f,save_in_file="shipment_inland",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name=str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_shipment_to_island(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
    except IndexError as e:
        try:
            tables = read_pdf(str(f),pages="1-2",area=(90, 0, 96, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_shipment_to_island(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
        except IndexError as e:
            
            tables = read_pdf(str(f),pages="1-2",area=(89, 0, 96, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_shipment_to_island(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
            

            

         
                
        

          
         
           





#process table one
def process_shipment_to_island(tables,pathname,date_index,table_index=0,filename="shipment_inland"):
    """
    Description
    -----------

    function processes the western costal shipment data 


    Parameters
    -----------

    tables:list() of table dfs
    pathname:list splitted file path
    date:(str) date abstracted from production
    table_index index of table to abstract data from
    filename:(str) name of file to save the data in
    
    
    Returns:
    datetime
    
    """
    
    table=tables[table_index]


  
    if table.shape[1] in [10,11] :

        table=table.dropna(thresh=table.shape[1]-4, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        prod_df=table.iloc[:,[0,5]]
        colmns=prod_df.columns.values
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)

        columns=prod_df.iloc[0].values
        

        if 'Unnamed: 0' in columns: 
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values
    

        check =  all(item in columns for item in ['Northeast', 'Midwest', 'South', 'West', 'Total'])


           
        
        if  not check:
            raise IndexError
        
        prod_df.columns=prod_df.iloc[0].values
        
        prod_df=prod_df[1:]
        prod_df=format_data(prod_df,prod_df.columns.tolist())

        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","LumberFacts",filename+".csv",ShipmentInland)

    else:
        raise IndexError




def abstract_average_price_coast_region(tables,f,save_in_file="average_price_coast",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name=str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_avg_price_coastal(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=AveragePriceCoastal)
    except IndexError as e:
        try:
            tables = read_pdf(str(f),pages="2",area=(9, 0, 16, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_avg_price_coastal(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceCoastal)
        except IndexError as e:
            tables = read_pdf(str(f),pages="3",area=(9, 0, 16, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_avg_price_coastal(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceCoastal)
  
           
            
            # tables = read_pdf(str(f),pages="1-2",area=(9, 0, 29, 100),stream=True,relative_area=True)
            # table_to_csv(tables)
            # process_shipment_to_island(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
            


#process table one
def process_avg_price_coastal(tables,pathname,date_index,table_index=0,filename="average_price_coast",mapper=None):
    """
    Description
    -----------

    function processes the western costal shipment data 


    Parameters
    -----------

    tables:list() of table dfs
    pathname:list splitted file path
    date:(str) date abstracted from production
    table_index index of table to abstract data from
    filename:(str) name of file to save the data in
    
    
    Returns:
    datetime
    
    """
    
    table=tables[table_index]


  
    if table.shape[1] in [10,11] :

        table=table.dropna(thresh=table.shape[1]-4, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        prod_df=table.iloc[:,[0,5]]
        colmns=prod_df.columns.values
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)

        columns=prod_df.iloc[0].values
   

        if 'Coast Region' in columns: 
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values

        if 'Ponderosa Pine' in columns:
            check =  all(item in columns for item in  ['Ponderosa Pine', 'Douglas Fir & Larch-Dry', 'Douglas Fir & Larch Green','White Fir', 'Englemann Spruce' ,'Western Red Cedar','Whitewoods'])
            rename_dict={"Ponderosa Pine":"Ponderosa_Pine",'Douglas Fir & Larch-Dry':'Douglas_Fir_and_Larch_Dry','Douglas Fir & Larch Green':'Douglas_Fir_and_Larch_Green',"White Fir":"White_Fir",'Englemann Spruce':'Englemann_Spruce' ,'Western Red Cedar':'Western_Red_Cedar'}


        
        else:
            check =  all(item in columns for item in ['Douglas Fir, Dry', 'Douglas Fir, Green' ,'Douglas Fir, All', 'Hem-Fir, Dry', 'Hem-Fir, Green' ,'Hem-Fir, All'])
            rename_dict= {"Douglas Fir, Dry":"Douglas_Fir_Dry","Douglas Fir, Green":"Douglas_Fir_Green","Douglas Fir, All":"Douglas_Fir_All","Hem-Fir, Dry":"Hem_Fir_Dry","Hem-Fir, Green":"Hem_Fir_Green","Hem-Fir, All":"Hem_Fir_All"}




           
        
        if  not check:
            raise IndexError
        
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
        prod_df=format_data(prod_df,prod_df.columns.tolist(),float)
        prod_df.rename(columns=rename_dict,inplace=True)
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","LumberFacts",filename+".csv",mapper)

    else:
        raise IndexError






def abstract_average_price_island_region(tables,f,save_in_file="average_price_island",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name=str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_avg_price_coastal(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=AveragePriceIsland)
    except IndexError as e:
        try:
            tables = read_pdf(str(f),pages="2",area=(18, 0, 27, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_avg_price_coastal(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceIsland)
        except IndexError as e:
            
            tables = read_pdf(str(f),pages="3",area=(18, 0, 27, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_avg_price_coastal(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceIsland)
  
           
            
            # tables = read_pdf(str(f),pages="1-2",area=(9, 0, 29, 100),stream=True,relative_area=True)
            # table_to_csv(tables)
            # process_shipment_to_island(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
            


#process table one
def process_avg_price_coastal_doglus(tables,pathname,date_index,table_index=0,filename="average_price_coast_douglas",mapper=None):
    """
    Description
    -----------

    function processes the western costal shipment data 


    Parameters
    -----------

    tables:list() of table dfs
    pathname:list splitted file path
    date:(str) date abstracted from production
    table_index index of table to abstract data from
    filename:(str) name of file to save the data in
    
    
    Returns:
    datetime
    
    """
    
    table=tables[table_index]


  
    if table.shape[1] in [10,11] :

        table=table.dropna(thresh=table.shape[1]-4, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        prod_df=table.iloc[:,[0,5]]
        colmns=prod_df.columns.values
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)

        columns=prod_df.iloc[0].values


        if 'Coast Region' in columns or "Coast Hem-fir (Dry)" in columns: 
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values

        if "Unnamed: 4" in columns or 'White Woods' in columns:
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values

        if 'Ponderosa Pine (Dry)' in columns:
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values




        check =  all(item in columns for item in  ['2x4 Stand & Btr R/L', '2x10 No. 2 & Btr R/L'])
       

        
        if not check:
            check =  all(item in columns for item in ['2x4 Stand & Btr R/L',"2x4 Stud Stand & Btr (8')",'2x10 No. 2 & Btr R/L'])
            


        if not check:
            check =  all(item in columns for item in ['2 x4 Stand & Btr R/L', "2x4 Stud Stand & Btr (8')" ,'2x10 No. 2 & Btr R/L'] )
            
         

        if not check:
            check =  all(item in columns for item in ["2x4 Stud (8')", '2x10 No. 2 & Btr R/L'] )
            


        if not check:
            check =  all(item in columns for item in ["2x4 Stud (8')", "2x6 Stud (8')"])
            


        if not check:
            check =  all(item in columns for item in ['5/4 Moulding & Btr R/W' ,'5/4 No. 2 Shop R/W' ,'5/4 No. 3 Shop R/W','1x6 No. 2 & Btr Com R/L'])
            

            

        rename_dict={"2x4 Stand & Btr R/L":"Stand_and_Btr_RL_2x4","2x4 Stud Stand & Btr (8')":"Stud_Stand_and_Btr_8_2x4","2x10 No. 2 & Btr R/L":"No2_and_Btr_RL_2x10","2 x4 Stand & Btr R/L":"Stand_and_Btr_RL_2x4","2 x4 Stud Stand & Btr (8')":"Stud_Stand_and_Btr_8_2x4","2x4 Stud (8')":"Stud_8_2x4","2x6 Stud (8')":"Stud_8_2x6","2 x4 Stud (8')":"Stud_8_2x4","2 x6 Stud (8')":"Stud_8_2x6","5/4 Moulding & Btr R/W":"Moulding_and_Btr_RW_5_4","5/4 No. 2 Shop R/W":"No_2_Shop_RW_5_4","5/4 No. 3 Shop R/W":"No_3_Shop_RW_5_4",'1x6 No. 2 & Btr Com R/L':'No_2_and_Btr_Com_RL_1x6'}
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
           
        
        if  not check:
            raise IndexError
        
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
       
        prod_df=format_data(prod_df,prod_df.columns.tolist(),float)
        prod_df.rename(columns=rename_dict,inplace=True)
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","LumberFacts",filename+".csv",mapper)

    else:
        raise IndexError






def abstract_average_price_costal_doglus(tables,f,save_in_file="average_price_costal_douglus",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name=str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_avg_price_coastal_doglus(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=AveragePriceCostalDoglas)
    except IndexError as e:
    

        try:
            tables = read_pdf(str(f),pages="2",area=[(32, 0,38 , 100)],stream=True,relative_area=True)
            table_to_csv(tables)
            process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceCostalDoglas)

        except IndexError as e:
            tables = read_pdf(str(f),pages="3",area=[(32, 0,38 , 100)],stream=True,relative_area=True)
            table_to_csv(tables)
            process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceCostalDoglas)

        

    
        # try:
        #     tables = read_pdf(str(f),pages="2",area=(29, 0, 33, 100),stream=True,relative_area=True)
        #     
        # except IndexError as e:
            
        #     tables = read_pdf(str(f),pages="3",area=(29, 0, 33, 100),stream=True,relative_area=True)
        #     table_to_csv(tables)
        #     process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
  
           
            
            # tables = read_pdf(str(f),pages="1-2",area=(9, 0, 29, 100),stream=True,relative_area=True)
            # table_to_csv(tables)
            # process_shipment_to_island(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
            

            







def abstract_average_price_costal_ham_fir(tables,f,save_in_file="average_price_costal_ham_fir",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name=str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_avg_price_coastal_doglus(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=AveragePriceCostalHamfir)
    except IndexError as e:
        try:
            tables = read_pdf(str(f),pages="2",area=[(36, 0,40 , 100)],stream=True,relative_area=True)
            table_to_csv(tables)
            process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceCostalHamfir)

        except IndexError as e:
            try:
            
                tables = read_pdf(str(f),pages="2",area=[(37, 0,42,100)],stream=True,relative_area=True)
                table_to_csv(tables)
                process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceCostalHamfir)
            except IndexError as e:
                try:
                    tables = read_pdf(str(f),pages="2",area=[(38, 0,45,100)],stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceCostalHamfir)

                except IndexError as e:
                    tables = read_pdf(str(f),pages="3",area=[(37, 0,42,100)],stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceCostalHamfir)
        



def abstract_average_price_island_dauglas_fir(tables,f,save_in_file="average_price_island_douglas_fir",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name=str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_avg_price_coastal_doglus(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=AveragePriceIslandDouglas)
    except IndexError as e:
        try:
            tables = read_pdf(str(f),pages="2",area=[(42, 0,46 , 100)],stream=True,relative_area=True)
            table_to_csv(tables)
            process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceIslandDouglas)

        except IndexError as e:
            try:
                tables = read_pdf(str(f),pages="2",area=[(40, 0,44 , 100)],stream=True,relative_area=True)
                table_to_csv(tables)
                process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceIslandDouglas)

            except IndexError as e:    
                try:
            
                    tables = read_pdf(str(f),pages="3",area=[(42, 0,46,100)],stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceIslandDouglas)
                except IndexError as e:
                
                    try:
                        tables = read_pdf(str(f),pages="3",area=[(41, 0,45,100)],stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceIslandDouglas)

                    except Exception  as e:
                
                        tables = read_pdf(str(f),pages="3",area=[(38, 0,43,100)],stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceIslandDouglas)
            

            


def abstract_average_price_white_woods(tables,f,save_in_file="average_price_white_woods",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name=str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_avg_price_coastal_doglus(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=AveragePriceWhiteWoods)
    except IndexError as e:
  
        try:
            tables = read_pdf(str(f),pages="2",area=[(44, 0,47 , 100)],stream=True,relative_area=True)
            table_to_csv(tables)
            process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceWhiteWoods)

        except IndexError as e:
            try:

                tables = read_pdf(str(f),pages="2",area=[(49, 0,51 , 100)],stream=True,relative_area=True)
                table_to_csv(tables)
                process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceWhiteWoods)

            
            except IndexError as e:
                try:

                    tables = read_pdf(str(f),pages="2",area=[(47, 0,49 , 100)],stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceWhiteWoods)

            
                except IndexError as e:
                
                    
    
                    try:
                    
                        tables = read_pdf(str(f),pages="3",area=[(44, 0,46,100)],stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceWhiteWoods)
                    except IndexError as e:
                        
                        try:
                            tables = read_pdf(str(f),pages="3",area=[(46, 0,49,100)],stream=True,relative_area=True)
                            table_to_csv(tables)
                            process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceWhiteWoods)

                        except IndexError as e:
                            tables = read_pdf(str(f),pages="3",area=[(45, 0,50,100)],stream=True,relative_area=True)
                            table_to_csv(tables)
                            process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePriceWhiteWoods)

                            # tables = read_pdf(str(f),pages="3",area=[(38, 0,43,100)],stream=True,relative_area=True)
                #         table_to_csv(tables)
                #         process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
            

    



def abstract_average_price_ponderaso_pine(tables,f,save_in_file="average_price_ponderaso_pine",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name=str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_avg_price_coastal_doglus(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=AveragePricePonderasoPine)
    except IndexError as e:
  
        try:
            tables = read_pdf(str(f),pages="2",area=[(48, 0,55 , 100)],stream=True,relative_area=True)
            table_to_csv(tables)
            process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePricePonderasoPine)

        except IndexError as e:
        
            # try:

            #     tables = read_pdf(str(f),pages="2",area=[(45, 0,57 , 100)],stream=True,relative_area=True)
            #     table_to_csv(tables)
            #     process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)

            
            # except IndexError as e:
            #     try:

            #         tables = read_pdf(str(f),pages="2",area=[(51, 0,56 , 100)],stream=True,relative_area=True)
            #         table_to_csv(tables)
            #         process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)

            
            #     except IndexError as e:
            #         raise e
                
                    
    
                    try:
                    
                        tables = read_pdf(str(f),pages="3",area=[(50, 0,54,100)],stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePricePonderasoPine)
                    except IndexError as e:
                        
                        
                        try:
                            tables = read_pdf(str(f),pages="3",area=[(51, 0,56,100)],stream=True,relative_area=True)
                            table_to_csv(tables)
                            process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePricePonderasoPine)

                        except IndexError as e:
                          
                            tables = read_pdf(str(f),pages="3",area=[(47, 0,56,100)],stream=True,relative_area=True)
                            table_to_csv(tables)
                            process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index,mapper=AveragePricePonderasoPine)

                            # tables = read_pdf(str(f),pages="3",area=[(38, 0,43,100)],stream=True,relative_area=True)
                #         table_to_csv(tables)
                #         process_avg_price_coastal_doglus(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)


# western_path="F:\\Traders\\2x4\\2x4 v2\\Data\\Fundamentals\\Historical Data 10 Years\\Historical Data 10 Years\\Western Lumber Facts"
# call to fuctions
# st=time.time()
# print("processing started") 

# pdfs=fetch_files(PDF_DIR=western_path)
# abstract_data(pdfs,save_in_file="average_price_ponderaso_pine",date_only=False)
# print("time taken:",time.time()-st)