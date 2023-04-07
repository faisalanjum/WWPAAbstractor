
from logging import exception
from math import prod
import os
from pathlib import Path

from numpy import NaN
import pandas as pd

import time
from backend.db.models import *
from tabula import read_pdf
from tabulate import tabulate
from common import save_to_dir,table_to_csv,fetch_files,check_if_exists,IGNORE_LIST

def abstract_barometer_data(pdf_files,save_in_file="finished_inventory_barometer"):
    for i,f in enumerate(pdf_files):
        f_name= str(f)
        f_path="DATA/Barometer/"+save_in_file+".csv"
        check_existance=check_if_exists(f_path,f_name)
       
        if check_existance:
            print("Data from File already exists")
            continue
     
        try:
            data_table= read_pdf(str(f),pages="2",area=(17, 0, 24, 100),stream=True,relative_area=True)
            date_table = read_pdf(str(f),pages="1",area=(1, 0, 25, 100),stream=True,relative_area=True)
            
            table_to_csv(data_table)
            abstract_barometer_date(date_table[0],data_table[0],f_name,save_in_file)
           

                    


        except Exception as e:
            pass




       

        
        
        



def abstract_barometer_date(date_table,data_table,f_name,save_to_file):

    columns=date_table.columns.tolist()
  
    date_=columns[1].split(":")

    date_=date_[1]
    prod_df=data_table.iloc[:,[0,1]]
    prod_df=prod_df.transpose()
    prod_df.reset_index(inplace=True)
    prod_df.columns=prod_df.iloc[0].values
    if "Total Inventory" in prod_df.columns.tolist():
        prod_df=prod_df.iloc[:,1:]

    prod_df=prod_df.drop(0)

    prod_df=format_data(prod_df,prod_df.columns.tolist(),float)

    prod_df["File"]=f_name
    prod_df["date"]=date_
    prod_df['Timestamp'] =  pd.to_datetime(prod_df['date'],format="%B %d, %Y").dt.normalize()
    prod_df['Year'] = pd.DatetimeIndex(prod_df['Timestamp']).year
    prod_df["Month"]=pd.DatetimeIndex(prod_df['Timestamp']).month
    prod_df["Day"]=pd.DatetimeIndex(prod_df['Timestamp']).day

    prod_df.rename(columns={'Coast Region':'Coast_Region','Inland Region':"Inland_Region",'Western Totals':"Western_Totals"},inplace=True)



    save_to_dir(prod_df,"DATA","Barometer",filename=save_to_file+".csv",mapper=BarometerFinishedInventories,process=False)

 
    











def abstract_data(pdf_files:list,save_in_file="lumber_consumption_canada",date_only=False):
    for i,f in enumerate(pdf_files):
        f_name= str(f)
        f_path="DATA/Lumber/"+save_in_file+".csv"
        print(f_path)
        check_existance=check_if_exists(f_path,f_name)
     
        

    

        if not check_existance :
            file_name=f_name
            
            check_ignore= str(file_name[-1])  in IGNORE_LIST
            
            if not check_ignore:
                production_us = read_pdf(str(f),pages="1-3",area=(17, 0, 25, 100),stream=True,relative_area=True)
                shipment_us=read_pdf(str(f),pages="1-3",area=[(24, 0, 32, 100)],stream=True,relative_area=True)
                orders_us=read_pdf(str(f),pages="1-3",area=(30, 0, 40, 100),stream=True,relative_area=True)
                pppc_table = read_pdf(str(f),pages="1-3",area=(80, 0, 92, 68),stream=True,relative_area=True)
                
                
                
                
                unfilled_orders_us=read_pdf(str(f),pages="1-3",area=(40, 0, 47, 48),stream=True,relative_area=True)
                inventories_us=read_pdf(str(f),pages="1-3",area=(47, 0, 54, 48),stream=True,relative_area=True)
                
                
                production_canada=read_pdf(str(f),pages="1-3",area=(60, 0, 66,100),stream=True,relative_area=True)
                shipments_canada=read_pdf(str(f),pages="1-3",area=(66, 0, 72,100),stream=True,relative_area=True)
                inventories_canada=read_pdf(str(f),pages="1-3",area=(70, 0, 77, 48),stream=True,relative_area=True)
                pppc_table = read_pdf(str(f),pages="1-3",area=(80, 0, 92, 68),stream=True,relative_area=True)
                
                
                
                
                imports_us=read_pdf(str(f),pages="2",area=(85, 0, 100, 100),stream=True,relative_area=True)
                export_us=read_pdf(str(f),pages="2",area=(19, 0,30, 100),stream=True,relative_area=True)
                import_log_us=read_pdf(str(f),pages="2",area=(31, 0,37, 100),stream=True,relative_area=True)
                export_log_us=read_pdf(str(f),pages="2",area=(37, 0,43, 100),stream=True,relative_area=True)
                export_lumber_canada=read_pdf(str(f),pages="2-3",area=(46, 0,52, 100),stream=True,relative_area=True)
                
                
                lumber_consumption_us=read_pdf(str(f),pages="2-3",area=(57, 0,64, 100),stream=True,relative_area=True)
                lumber_consumption_canada=read_pdf(str(f),pages="2-3",area=(78, 0,84, 100),stream=True,relative_area=True)
                
                try:
                    north_american_production=read_pdf(str(f),pages="3",area=(15, 0,30, 100),stream=True,relative_area=True)
                except Exception as e:
                    try:
                        north_american_production=read_pdf(str(f),pages="4",area=(15, 0,30, 100),stream=True,relative_area=True)
                    except Exception as e:
                        pass


                try:
                    north_american_shipment=read_pdf(str(f),pages="4",area=(34, 0,50, 100),stream=True,relative_area=True)
                except Exception as e:
                    try:
                        north_american_shipment=read_pdf(str(f),pages="3",area=(34, 0,50, 100),stream=True,relative_area=True)
                    except Exception as e :
                        pass

                
                try:
                    north_american_orders=read_pdf(str(f),pages="4",area=(54, 0,65, 100),stream=True,relative_area=True)
                except Exception as e:
                    north_american_orders=read_pdf(str(f),pages="3",area=(54, 0,65, 100),stream=True,relative_area=True)

                try:
                    north_american_unfilled_orders=read_pdf(str(f),pages="4",area=(67, 0,76, 100),stream=True,relative_area=True)
                except Exception as e:
                    north_american_unfilled_orders=read_pdf(str(f),pages="3",area=(67, 0,76, 100),stream=True,relative_area=True)

                try:
                    north_american_inventory=read_pdf(str(f),pages="4",area=(79, 0,100, 100),stream=True,relative_area=True)
                except Exception as e:
                    try:
                        north_american_inventory=read_pdf(str(f),pages="3",area=(79, 0,100, 100),stream=True,relative_area=True)
                    except Exception as e:
                        raise e 



                table_to_csv(export_us)

            
                
                # process production data
                
                date=abstract_production( production_us,f,just_date=date_only)
                abstract_shipment( shipment_us,f,date_index=date)
                abstract_orders(orders_us,f,date_index=date)
                abstract_unfilledorders(unfilled_orders_us,f,date_index=date)
                abstract_inventory(inventories_us,f,date_index=date)

                
                abstract_production_canada(production_canada,f,date_index=date)
                abstract_shipment_canada(shipments_canada,f,date_index=date)
                abstract_inventory_canada(inventories_canada,f,date_index=date)
                abstract_pppc(pppc_table,f,date_index=date)
                
                abstract_imports(imports_us,f,date_index=date)
                abstract_exports(export_us,f,date_index=date)
                abstract_log_exports(export_log_us,f,date_index=date)
                abstract_log_imports(import_log_us,f,date_index=date)
                abstract_lumber_exports_canada(export_lumber_canada,f,date_index=date)

                abstract_lumber_consumption_us(lumber_consumption_us,f,date_index=date)
                abstract_lumber_consumption_canada(lumber_consumption_canada,f,date_index=date)
                
                abstract_north_american_production(north_american_production,f,date_index=date)
                abstract_north_american_shipment(north_american_shipment,f,date_index=date)
                abstract_north_american_orders(north_american_orders,f,date_index=date)
                abstract_north_american_inventory(north_american_inventory,f,date_index=date)
                abstract_north_american_unfilled_orders(north_american_unfilled_orders,f,date_index=date)
                    
        
        else:
            print("Record Already Update ")

            
      
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
        
    f_name= str(f)
        
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        date=process_production(tables,pathname=f_name,filename=save_in_file,date_only=just_date)
    except IndexError as e:
        try:
                date=process_production(tables,pathname=f_name,table_index=1,filename=save_in_file,date_only=just_date)
        except IndexError as e:
            try:
                date=process_production(tables,pathname=f_name,table_index=2,filename=save_in_file,date_only=just_date)

            except IndexError as e:
                try:
                    tables=read_pdf(str(f),pages="1-3",area=(7, 0, 14, 100),stream=True,relative_area=True)
                    table_to_csv(tables)
                    date=process_production(tables,pathname=f_name,table_index=1,filename=save_in_file,date_only=just_date)
                except IndexError as e:
                    pass
    return date


def abstract_production_canada(tables,f,save_in_file="production_canada",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path


    Returns:
    --------

    date:datetime 
    
    """
    
    
    f_name= str(f)
        
    
    
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        date=process_production_canada(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
    except IndexError as e:
        try:
                date=process_production_canada(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
        except IndexError as e:
            try:
                date=process_production_canada(tables,pathname=f_name,table_index=2,filename=save_in_file,date_index=date_index)

            except IndexError as e:
                
                try:
                    tables=read_pdf(str(f),pages="1-3",stream=True,area=((47, 0, 54,100)),relative_area=True)
                    table_to_csv(tables)
                    date=process_production_canada(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
                
                except IndexError as e:

                    try:
                        tables=read_pdf(str(f),pages="1-3",stream=True,multiple_tables=True,area=((48, 0, 53,100)),relative_area=True)
                        table_to_csv(tables)
                        date=process_production_canada(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)

                    except Exception as e:
                        try:
                            tables=read_pdf(str(f),pages="1-3",stream=True,multiple_tables=True,area=((49, 0, 53,100)),relative_area=True)
                            table_to_csv(tables)
                            date=process_production_canada(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)

                        except Exception as e:
                            try:
                                tables=read_pdf(str(f),pages="1-3",stream=True,multiple_tables=True,area=((50, 0, 54,100),),relative_area=True)
                                table_to_csv(tables)
                                date=process_production_canada(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
                            except IndexError as e:
                                pass
                                
                


#process table one
def process_production(tables,pathname,table_index=0,filename="production",date_only=False):
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
    table.rename( columns={'Unnamed: 0':'date'}, inplace=True )
    prod_df=table.iloc[:,[1,6]]
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
    if "Other R" in columns:
        prod_df.rename(columns={"Other R": "Other"},inplace=True)
    prod_df=format_data(prod_df,["West","South","Other","Total"])
    prod_df["File"]=pathname
    save_to_dir(prod_df,"DATA","Lumber",filename+".csv",mapper=ProductionUS)
    return date





# function removes the spaces and comma's from columns of df

def format_data(df,columns,format_type=int):
        df_cpy=df
        skip_col=["date","File"]
  
        for col in columns:

            if col in skip_col:
                continue




            try:
            
                df_cpy[col]=df_cpy[col].str.replace("$",'').str.replace(",","").str.replace('(', '').str.replace("%","").str.replace(")",'').str.replace(',', '').str.replace("%","").str.replace(" ",'').astype(format_type)

              
          
            except ValueError:
                try:
                    if format_type == int:
                        
                        val=df_cpy[col].tolist()
                    
                     
                        value=str(val[0]).split(".")
                        if 'N/A' in value or "NA" in value or "na" in value or "nan" in value or "N/A" in value:
                             df_cpy[col]="00"
                        
                        df_cpy[col]=value[0]
                   
                    
                    
                    else:
                        val=df_cpy[col].tolist()
                        value=str(val[0]).split(".")


                        
                        if 'N/A' in value or "NA" in value or "na" in value or "nan" in value :
                             df_cpy[col]="00.00"

                        
                       


                        
                        
                        else:
                            df_cpy[col]=value[0]+"."+value[1]
                        
                     
                    df_cpy=df_cpy.fillna(0.00) 

                    df_cpy[col]=df_cpy[col].str.replace(",","")
                    df_cpy[col]=df_cpy[col].str.replace("N/A","000")
                    df_cpy[col]=df_cpy[col].str.replace("NA","000")

                    
                    df_cpy[col]=df_cpy[col].str.replace('N/A',"000").str.replace("nan","000").str.replace('(', '').str.replace("%","").str.replace(")",'').str.replace("$",'').str.replace(',', '').str.replace("%","").str.replace(" ",'').astype(format_type)
                
                
                except IndexError as e:


                    df_cpy[col]=df_cpy[col].str.replace(",","")
                    df_cpy[col]=df_cpy[col].str.replace("N/A","000")
                    df_cpy[col]=df_cpy[col].str.replace("NA","000")
                    df_cpy[col]=df_cpy[col].str.replace('N/A',"000").str.replace("nan","000").str.replace('(', '').str.replace("%","").str.replace(")",'').str.replace("$",'').str.replace(',', '').str.replace("%","").str.replace(" ",'').astype(format_type)





                    df_cpy[col]=df_cpy[col].str.replace("$",'').str.replace('(', '').str.replace("%","").str.replace(")",'').str.replace(",","").str.replace(" ","").astype(format_type)
             


            except AttributeError:
                return df 
        
        return df_cpy
        








#function below abstracts the  shipment data from the abstracted tables

def abstract_shipment(tables,f,save_in_file="shipment_us",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        date=process_shipment(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
    except IndexError as e:
        try:
                date=process_shipment(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
        except IndexError as e:
            try:
                date=process_shipment(tables,pathname=f_name,table_index=2,filename=save_in_file,date_index=date_index)

            except IndexError as e:
               
       
                try:
                    tables=read_pdf(str(f),pages="2",area=[(14, 0, 19, 100)],stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_shipment(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                except IndexError as e:
                    tables=read_pdf(str(f),pages="1",area=[(26, 0, 31, 100)],stream=True,relative_area=True)
                    table_to_csv(tables)
                  
                    process_shipment(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
               
                     
    




def abstract_shipment_canada(tables,f,save_in_file="shipment_canada",date_index=None):
    """
    PARAMETERS
    -------------------------------------
    tables : list list of tables abstracted
    f_name : file path
    
    Returns:
    --------
    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_shipment_canada(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
    except IndexError as e:
        try:
                process_shipment_canada(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
        except IndexError as e:
            try:
                process_shipment_canada(tables,pathname=f_name,table_index=2,filename=save_in_file,date_index=date_index)

            except IndexError as e:
                try:
                    tables=read_pdf(str(f),pages="1-3",area=[(54, 0, 59, 100)],stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_shipment_canada(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
                except IndexError as e:
                   raise e
    

















#process table one
def process_shipment(tables,pathname,date_index,table_index=0,filename="shipment_us"):
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
    if table.shape[1] in [13,14]:

        

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
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]

      
        if "Other R" in columns:

           
            prod_df.rename(columns={'Other R':'Other'},inplace=True)
            
            columns=prod_df.columns.tolist()
        
     

        check =  all(item in  ['West', 'South', 'Other' ,'Total'] for item in columns)
        
        
        
        if  not check:
            raise IndexError
        
      
    
        prod_df["date"]=date_index
        prod_df["File"]=pathname

        prod_df=format_data(prod_df,['West', 'South', 'Other' ,'Total'])
        save_to_dir(prod_df,"DATA","Lumber",filename+".csv",ShipmentUS)

    else:
        raise IndexError






#process table one
def process_shipment_canada(tables,pathname,date_index,table_index=0,filename="shipment_canada"):
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
    if table.shape[1] in [13,14,15] :
        
        table=table.dropna(thresh=table.shape[1]-4, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        table.columns=table.iloc[0,:].values

        try:
            table=table.drop(0)
        except KeyError as e:
            table=table.drop(1)
            
        prod_df=table.iloc[:,[0,5]]
        colmns=prod_df.columns.values
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)
        columns=prod_df.iloc[0].values
        check =  all(item in columns for item in ['British Columbia', 'East of the Rockies', 'Total'])
        
        if  not check:
            raise IndexError
        
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
        prod_df=format_data(prod_df,prod_df.columns.tolist())
        prod_df.rename(columns={'British Columbia':'British_Columbia', 'East of the Rockies':'East_of_the_Rockies'},inplace=True)
   
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","Lumber",filename+".csv",ShipmentCanada)

    else:
        raise IndexError




def process_production_canada(tables,pathname,table_index=0,filename="production_ca",col_index=5,date_index=None):
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
    
    if table.shape[1] in [12,13,14]:
        
        if table.shape[0]>=4:
            table.columns=table.iloc[0,:].values
            table=table.drop(0)
            table=table.dropna(thresh=table.shape[1]-4, axis=0)
            table=table.dropna(thresh=table.shape[0]-3, axis=1)
        
        if table.shape[1] == 13:
            col_index=6

        
        elif table.shape[1] == 12:
            col_index=4
        
        prod_df=table.iloc[:,[0,col_index]]
        colmns=prod_df.columns.values
        date=date_index

    



       
        
        
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)
        columns=prod_df.iloc[0].tolist()
        print(columns)
        print(prod_df.head())
        if "Production" in columns:
            
            prod_df=prod_df.iloc[:,1:]
            prod_df.columns=prod_df.iloc[0].values
            
        else:
            prod_df.columns=columns
        
        ref=['British Columbia', 'East of the Rockies', 'Total']

    
        print(prod_df.columns.tolist())
      

        check =  all(item in ref for item in prod_df.columns.tolist())
        
        if  not check or len(prod_df.columns.tolist()) !=len(ref):
            raise IndexError
        
        print(prod_df.head())

        prod_df=prod_df.iloc[1:]



        prod_df=format_data(prod_df,prod_df.columns.tolist())
        prod_df.rename(columns={'British Columbia':'British_Columbia', 'East of the Rockies':'East_of_the_Rockies'},inplace=True)
      
        prod_df["date"]=date
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","Lumber",filename+".csv",ProductionCanada)

    else:
     
        raise IndexError





#function below abstracts the  shipment data from the abstracted tables

def abstract_orders(tables,f,save_in_file="orders_us",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_orders(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
    except IndexError as e:
        try:
                process_orders(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
        except IndexError as e:
            try:
                process_orders(tables,pathname=f_name,table_index=2,filename=save_in_file,date_index=date_index)

            except IndexError as e:
                try:
                    tables=read_pdf(str(f),pages="1-3",area=[(21, 0, 26, 100)],stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_orders(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
                except IndexError as e:
                    try:
                        process_orders(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)

                    except Exception as e:
                        tables=read_pdf(str(f),pages="1",area=[(32, 0, 39, 100)],stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_orders(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
                

               
                   
    


#process table one
def process_orders(tables,pathname,date_index,table_index=0,filename="order_us"):
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
   
    if table.shape[1] in [11,12,13,14,15,16] and table.shape[0] > 3:
        table=table.dropna(thresh=table.shape[1]-4, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)

        

        try:
            table.columns=table.iloc[0].values
            table=table.drop(0)
        except KeyError as e:
            table=table.drop(1)
            
        prod_df=table.iloc[:,[0,5]]
        colmns=prod_df.columns.values
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)
        columns=prod_df.iloc[0].values
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
   

        if  "Other R" in columns:
            prod_df.rename(columns={"Other R":"Other",},inplace=True)
            columns=prod_df.columns.tolist()
     
        check =  all(item in columns for item in ['West', 'South', 'Other' ,'Total'])
        
        
        if  not check:
            raise IndexError
        
        
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        prod_df=format_data(prod_df,['West', 'South', 'Other' ,'Total'])
        save_to_dir(prod_df,"DATA","Lumber",filename+".csv",mapper=OrdersUS)

    else:
        raise IndexError


            








def abstract_unfilledorders(tables,f,save_in_file="unfilled_orders_us",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_unfilledorders(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
    except IndexError as e:
        
        try:
            process_unfilledorders(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
        
        
        except IndexError as e:
            try:
                process_unfilledorders(tables,pathname=f_name,table_index=2,filename=save_in_file,date_index=date_index)

            except IndexError as e:
                try:
                    tables=read_pdf(str(f),pages="1-3",area=[(28, 0, 35, 45)],stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_unfilledorders(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
                except IndexError as e:
                    tables=read_pdf(str(f),pages="1-3",area=[(26, 0, 33, 45)],stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_unfilledorders(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
    



#process table one
def process_unfilledorders(tables,pathname,date_index,table_index=0,filename="order_us"):
    """
    Description
    -----------

    function processes the us unfilles orders data 


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
    
    if table.shape[1] in [5,4] :
      
        table=table.dropna(thresh=table.shape[1]-4, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)

        

        try:
            table.columns=table.iloc[0].values
            table=table.drop(0)
        except KeyError as e:
            table=table.drop(1)
        
        prod_df=table.iloc[:,[0,2]]
        
        colmns=prod_df.columns.values
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)
        prod_df.columns=prod_df.iloc[0].values

        columns=prod_df.columns.tolist()
    

        if "Other R" in columns:
      
            prod_df.rename(columns={"Other R":"Other"},inplace=True)
            columns=prod_df.columns.tolist()

        check =  all(item in ['West', 'South', 'Other' ,'Total'] for item in columns )
        
        if  not check:
            raise IndexError
        
        prod_df=prod_df[1:]
     
        prod_df=format_data(prod_df,['West', 'South', 'Other' ,'Total'])
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","Lumber",filename+".csv",UnfilledOrdersUS)

    else:
        raise IndexError






def abstract_inventory(tables,f,save_in_file="inventory_us",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_inventory(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
    except IndexError as e:
        
        try:
            process_inventory(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
        
        
        except IndexError as e:
            try:
                process_inventory(tables,pathname=f_name,table_index=2,filename=save_in_file,date_index=date_index)

            except IndexError as e:
                try:
                    tables=read_pdf(str(f),pages="1-3",area=[(35, 0, 42, 45)],stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_inventory(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
                except IndexError as e:
                    try:
                        tables=read_pdf(str(f),pages="1-3",area=(47, 0, 56, 48),stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_inventory(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                    except IndexError as e:

                        try:
                            process_inventory(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
                        except IndexError as e:
                            try:
                                process_inventory(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                            except Exception as e:
                                tables=read_pdf(str(f),pages="2",area=(35, 0, 42, 45),stream=True,relative_area=True)
                                table_to_csv(tables)
                                process_inventory(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                   

                      

                  

              
    



#process table one
def process_inventory(tables,pathname,date_index,table_index=0,filename="inventory"):
    """
    Description
    -----------

    function processes the us unfilles orders data 


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
    
    if table.shape[1] in [3,5,4,6,7] :
        
        table=table.dropna(thresh=table.shape[1]-1, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        
        try:
         
            table.columns=table.iloc[0].values
            table=table.drop(0)
        except KeyError as e:
            try:
                table=table.drop(1)
            except Exception as e:
                raise IndexError
        
        prod_df=table.iloc[:,[0,2]]
        colmns=prod_df.columns.values
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
        columns=prod_df.columns.tolist()

        
        if "Other R" in columns:
            prod_df.rename(columns={"Other R":"Other"},inplace=True)
            columns=prod_df.columns.tolist()
 
           
   
        check =  all(item in columns for item in ['West', 'South', 'Other' ,'Total'])

        
        if  not check:
            raise IndexError

        prod_df=format_data(prod_df,['West', 'South', 'Other' ,'Total'])
      
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","Lumber",filename+".csv",InventoryUS)

    else:
        raise IndexError














def abstract_inventory_canada(tables,f,save_in_file="inventory_canada",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_inventory_canada(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
    except IndexError as e:
        
        try:
            process_inventory_canada(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
        
        
        except IndexError as e:
            try:
                process_inventory_canada(tables,pathname=f_name,table_index=2,filename=save_in_file,date_index=date_index)

            except IndexError as e:
                
                    try:
                        tables=read_pdf(str(f),pages="1-3",area=(58, 0, 64, 48),stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_inventory_canada(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                    except IndexError as e:
                        try:
                            process_inventory_canada(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
                        except IndexError as e:
                            try:
                                tables=read_pdf(str(f),pages="1-3",area=(72, 0, 77, 60),stream=True,relative_area=True)
                                table_to_csv(tables)
                                process_inventory_canada(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                            except Exception as e:
                                raise e 
                        
                    

                  

              




#process table one
def process_inventory_canada(tables,pathname,date_index,table_index=0,filename="inventory_canada"):
    """
    Description
    -----------

    function processes the us unfilles orders data 


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
    if table.shape[1] in [5,4,6] :

        
      
        table=table.dropna(thresh=table.shape[1]-4, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        
        try:
            table.columns=table.iloc[0].values
            table=table.drop(0)
        except KeyError as e:
            table=table.drop(1)
        
        prod_df=table.iloc[:,[0,2]]
        colmns=prod_df.columns.values
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)
        columns=prod_df.iloc[0].values

        check =  all(item in columns for item in ['British Columbia', 'East of the Rockies', 'Total'])
    


        
        if  not check:
            raise IndexError
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
        if "Inventories" in prod_df.columns.tolist():
            prod_df.drop("Inventories",axis=1,inplace=True)
        prod_df=format_data(prod_df,prod_df.columns.tolist())
        prod_df.rename(columns={'British Columbia':'British_Columbia', 'East of the Rockies':'East_of_the_Rockies'},inplace=True)
   
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","Lumber",filename+".csv",InventoryCanada)

    else:
        raise IndexError



def abstract_pppc(tables,f,save_in_file="pppc",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_pppc(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
    except IndexError as e:
        
        try:
            process_pppc(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
        
        
        except IndexError as e:
            try:
                process_pppc(tables,pathname=f_name,table_index=2,filename=save_in_file,date_index=date_index)

            except IndexError as e:
                try:
                    tables=read_pdf(str(f),pages="1-3",area=(68, 0, 77, 56),stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_pppc(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
                
                except IndexError as e:
                    
                    try:
                        tables=read_pdf(str(f),pages="1-3",area=(82,0, 94 , 56),stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_pppc(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                    
                    except Exception as e:

                        tables=read_pdf(str(f),pages="1-3",area=(83,0, 94 , 56),stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_pppc(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                   

                   
                        

                  

              




#process table one
def process_pppc(tables,pathname,date_index,table_index=0,filename="pppc"):
    """
    Description
    -----------

    function processes the us unfilles orders data 


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





    if table.shape[1] in [6,7,8,9] :
        table=table.dropna(thresh=table.shape[1]-5, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        table.reset_index(inplace=True)



        if not table.shape[1] == 8:

            if table.shape[1] == 7:
                table.columns=table.iloc[1].values
                table=table.drop([0,1]) 
            
            else:
                table.columns=table.iloc[2].values
                table=table.drop([0,1,2])
        else:
            table.columns=table.iloc[1].values
            table=table.drop([0,1])

        


        table=table.drop_duplicates()
       
        if table.shape[0] in [5,6] and table.shape[1] == 7:
            prod_df=table.iloc[:,[1,4]]
        else:
            prod_df=table.iloc[:,[1,5]]
      

       
       
       
        colmns=prod_df.columns.values
        
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)
        
       
        prod_df.columns=prod_df.iloc[0].values
     
        if "4South" in prod_df.columns.tolist():
            prod_df.rename(columns={"4South":"South"},inplace=True)
            

        check =  all(item in prod_df.columns.tolist() for item in ['West','South','Total U.S.','British Columbia', 'East of the Rockies', 'Total Canada'])
        if  not check:
            raise IndexError
        
        prod_df.rename(columns={'Total U.S.':'Total_US','British Columbia':'British_Columbia', 
        'East of the Rockies':'East_of_the_Rockies', 'Total Canada':'Total_Canada'},inplace=True)
        prod_df=prod_df[1:]
        if NaN in prod_df.columns.tolist():
            prod_df=prod_df.iloc[:,1:]
        
        prod_df=format_data(prod_df,prod_df.columns.tolist())
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        
        if prod_df.shape[1] > 8:
            prod_df=prod_df.iloc[:,1:]



        save_to_dir(prod_df,"DATA","Lumber",filename+".csv",PPPC)

    else:
        raise IndexError


















def abstract_imports(tables,f,save_in_file="imports_us",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_imports(tables,pathname=f_name,filename=save_in_file,date_index=date_index)

    except IndexError as e:

 
        try:
            tables=read_pdf(str(f),pages="2-3",area=(10, 0, 20, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_imports(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
    
        except IndexError as e:
            try:
                process_imports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
            
            except IndexError as e:
                try:
                    tables=read_pdf(str(f),pages="2-3",area=(10, 0, 21, 100),stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_imports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                except IndexError as e:

                    try:
                        process_imports(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)

                    except IndexError as e:
                        try:
                            tables=read_pdf(str(f),pages="2",area=(9, 0,20 , 100),stream=True,relative_area=True)
                            table_to_csv(tables)
                            process_imports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)

                        except IndexError as e:
                            try:
                                tables=read_pdf(str(f),pages="2",area=(9, 0,22 , 100),stream=True,relative_area=True)
                                table_to_csv(tables)
                                process_imports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                            
                            except Exception as e:
                                tables=read_pdf(str(f),pages="2",area=(8, 0,23 , 100),stream=True,relative_area=True)
                                table_to_csv(tables)
                                process_imports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                            




               
                    
        
             
       
        
            
         
        
                    

                  

              




#process table one
def process_imports(tables,pathname,date_index,table_index=0,filename="imports_us"):
    """
    Description
    -----------

    function processes the us unfilles orders data 


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

    if table.shape[1] in [13,14,15,16,17,18]:
        table=table.dropna(thresh=table.shape[1]-5, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        table.reset_index(inplace=True)
        table=table.drop_duplicates()
        
        prod_df=table.iloc[:,[1,6]]
        colmns=prod_df.columns.values
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)
        columns=prod_df.iloc[0].values


        if 'Unnamed: 0' in columns: 
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values

        if  "1" in columns:
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values

        
        if len(columns) > 8:
            prod_df=prod_df.iloc[:,1:]
            prod_df=table.iloc[:,[1,6]]
            columns=prod_df.iloc[1].values

     
        check =  all(item in columns for item in ['From British Columbia','East of the Rockies','Total Canadian Imports','From Latin America','From Europe','Total Non-Canadian','Total Lumber Imports'])
        
        if  not check:
            raise IndexError
        
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]

        prod_df=format_data(prod_df,prod_df.columns.tolist())
        rename_dict={'From British Columbia':"From_British_Columbia",'East of the Rockies':'East_of_the_Rockies','Total Canadian Imports':'Total_Canadian_Imports','From Latin America':'From_Latin_America','From Europe':'From_Europe','Total Non-Canadian':'Total_Non_Canadian','Total Lumber Imports':'Total_Lumber_Imports'}
        prod_df.rename(columns=rename_dict,inplace=True)
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","Lumber",filename+".csv",LumberImportsUS)

    else:
        raise IndexError


def abstract_exports(tables,f,save_in_file="export_us",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_exports(tables,pathname=f_name,filename=save_in_file,date_index=date_index)

    except IndexError as e:


      
       

 
        try:
            tables=read_pdf(str(f),pages="3",area=(7, 0, 15, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_exports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
    
        
            
        except IndexError as e:

           
          
            try:

         
                tables=read_pdf(str(f),pages="3",area=(23, 0, 30, 100),stream=True,relative_area=True)
                table_to_csv(tables)
                process_exports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
        
            
            except IndexError as e:
                
                try:
                    tables=read_pdf(str(f),pages="2",area=(23, 0, 30, 100),stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_exports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                except IndexError as e:
                
                        try:
                            tables=read_pdf(str(f),pages="2-3",area=(30, 0,39, 100),stream=True,relative_area=True)
                            table_to_csv(tables)
                            process_exports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)

                        except IndexError as e:
                            try:
                                process_exports(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)

                            except IndexError as e:
                                try:
                                    tables=read_pdf(str(f),pages="2-3",area=(22, 0,30 , 100),stream=True,relative_area=True)
                                    table_to_csv(tables)
                                    process_exports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index) 

                                except IndexError as e: 
                                    process_exports(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)  

                    
                            




               
                    
        
             
       
        
            
         
        
                    

                  

              




#process table one
def process_exports(tables,pathname,date_index,table_index=0,filename="Exports_us"):
    """
    Description
    -----------

    function processes the us unfilles orders data 


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

    

    if table.shape[1] in [13,14,15,16,17,18]:
        table=table.dropna(thresh=table.shape[1]-5, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        table.reset_index(inplace=True)
        table=table.drop_duplicates()
        prod_df=table.iloc[:,[1,6]]
        colmns=prod_df.columns.values
        
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)
        columns=prod_df.iloc[0].values
        
        if 'Unnamed: 0' in columns: 
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values



    

        chck_ref=['To Canada', 'To China', 'To Japan', 'To Mexico' ,'To Other Countries','Total Lumber Exports']

        
       
     
        check =  all(item in columns for item in chck_ref)  #check if all the columns are present



        if  not check:
            chck_ref2=['To Canada', 'To Japan', 'To Mexico' ,'To Other Countries','Total Lumber Exports']
            filename = "Exports_us_2" 

            check =  all(item in columns for item in chck_ref2)  #check if all the columns are present

        if  not check:   
                
            raise IndexError
        
        prod_df.columns=prod_df.iloc[0].values

        print(prod_df.head())       
        prod_df=prod_df[1:]
        prod_df=format_data(prod_df,prod_df.columns.tolist())
        rename_dict={'To Canada':'To_Canada', 'To China':'To_China', 'To Japan':'To_Japan', 'To Mexico':'To_Mexico' ,'To Other Countries':'To_Other_Countries','Total Lumber Exports':'Total_Lumber_Export'}

        prod_df.rename(columns=rename_dict,inplace=True)
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","Lumber",filename+".csv",LumberExportUS)

    else:
        raise IndexError






def abstract_log_imports(tables,f,save_in_file="log_import_us",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_logs_imports(tables,pathname=f_name,filename=save_in_file,date_index=date_index)

    except IndexError as e:
    
        

 
        try:
            tables=read_pdf(str(f),pages="3",area=(31, 0, 36, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_logs_imports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)

        except IndexError as e:
        
            try:

         
                tables=read_pdf(str(f),pages="3",area=(15, 0, 20, 100),stream=True,relative_area=True)
                table_to_csv(tables)
                process_logs_imports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
        
            
            except IndexError as e:
                try:
                    tables=read_pdf(str(f),pages="2",area=(32, 0, 36, 100),stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_logs_imports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                
                except IndexError as e:
               
                
                    try:
                        tables=read_pdf(str(f),pages="3",area=(40,0, 46, 100),stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_logs_imports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                
                    except IndexError as e:
                        try:
                            tables=read_pdf(str(f),pages="2",area=(40,0, 44, 100),stream=True,relative_area=True)
                            table_to_csv(tables)
                            process_logs_imports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                        except IndexError as e:
                            tables=read_pdf(str(f),pages="3",area=(40,0, 45, 100),stream=True,relative_area=True)
                            table_to_csv(tables)
                            process_logs_imports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                  
                            
                    
                         
                       
#process table one
def process_logs_imports(tables,pathname,date_index,table_index=0,filename="log_imports_us"):
    """
     Description
    -----------
     function processes the us log import data 
     
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
    if table.shape[1] in [9,13,14,15,16,17,18]:
        table=table.dropna(thresh=table.shape[1]-5, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        table.reset_index(inplace=True)
        table=table.drop_duplicates()
        prod_df=table.iloc[:,[1,6]]
        colmns=prod_df.columns.values
        
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)
        columns=prod_df.iloc[0].values

        if 'Unnamed: 0' in columns: 
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values
        ref= ['From Canada' ,'Non-Canadian Sources' ,'Total Log Imports']
        rename_dict={'From Canada':'From_Canada' ,'Non-Canadian Sources':'Non_Canadian_Sources' ,'Total Log Imports':'Total_Log_Imports'}
        check =  all([item in columns for item in ref])
        if not check:
           
            raise IndexError
        
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
        prod_df=format_data(prod_df,prod_df.columns.tolist())
        prod_df.rename(columns=rename_dict,inplace=True)
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","Lumber",filename+".csv",LogImportsUS)

    else:
        raise IndexError





def abstract_log_exports(tables,f,save_in_file="log_export_us",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_logs_exports(tables,pathname=f_name,filename=save_in_file,date_index=date_index)

    except IndexError as e:
        try:
            tables=read_pdf(str(f),pages="3",area=(37, 0, 43, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_logs_exports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)

        except IndexError as e:
            
            try:
                tables=read_pdf(str(f),pages="2-3",area=(37, 0, 45, 100),stream=True,relative_area=True)
                table_to_csv(tables)
                process_logs_exports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
            
            except IndexError as e:
                try:
                    process_logs_exports(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
            
                
                except IndexError as e:
                    try:
                        tables=read_pdf(str(f),pages="3",area=(21,0, 27, 100),stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_logs_exports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                
                    except IndexError as e:
                        try:
                        
                            tables=read_pdf(str(f),pages="3",area=(21,0, 29, 100),stream=True,relative_area=True)
                            table_to_csv(tables)
                            process_logs_exports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)

                        except IndexError as e:

                            try:
                                tables=read_pdf(str(f),pages="3",area=(36,0, 42, 100),stream=True,relative_area=True)
                                table_to_csv(tables)
                                process_logs_exports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)

                            except Exception as e:
                                try:
                                    tables=read_pdf(str(f),pages="2",area=(37,0, 44, 100),stream=True,relative_area=True)
                                    table_to_csv(tables)
                                    process_logs_exports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                                
                                except Exception as e:
                                    try:
                                        tables=read_pdf(str(f),pages="2",area=(45,0, 52, 100),stream=True,relative_area=True)
                                        table_to_csv(tables)
                                        process_logs_exports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)

                                    except Exception as e:
                                        try:
                                            tables=read_pdf(str(f),pages="2-3",area=(46,0, 53, 100),stream=True,relative_area=True)
                                            table_to_csv(tables)
                                            process_logs_exports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                                        except Exception as e:

                                            handler(f,f_name,save_in_file,date_index)
                                            
                                            


def handler(f,f_name,save_in_file,date_index):
    try:
        tables=read_pdf(str(f),pages="2-3",area=(38,0, 44, 100),stream=True,relative_area=True)
        table_to_csv(tables)
        process_logs_exports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
    except IndexError as e:
        try:
            tables=read_pdf(str(f),pages="3",area=(46,0, 53, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_logs_exports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
        except IndexError as e:
            tables=read_pdf(str(f),pages="2",area=(46,0, 53, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_logs_exports(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
      


                                        

#process table one
def process_logs_exports(tables,pathname,date_index,table_index=0,filename="log_exports_us"):
    """
     Description
    -----------
     function processes the us us log export data 
     
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

    
    if table.shape[1] in [13,14,15,16,17,18]:
        table=table.dropna(thresh=table.shape[1]-5, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        table.reset_index(inplace=True)
        table=table.drop_duplicates()
        prod_df=table.iloc[:,[1,6]]
        colmns=prod_df.columns.values
        
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)
        columns=prod_df.iloc[0].values
  
        
        if 'Unnamed: 5' in columns: 
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values


        if len(columns) == 5:

            ref=['To China','To Japan', 'To Canada', 'To Other Countries', 'Total Log Exports']
            check =  all([item in columns for item in ref])
            rename_dict={'To China':'To_China','To Japan':'To_Japan', 'To Canada':'To_Canada', 'To Other Countries':'To_Other_Countries', 'Total Log Exports':'Total_Log_Export'}
            filename=filename +"_1"
           

        else:
            ref=['To Japan', 'To Canada', 'To Other Countries', 'Total Log Exports']
            rename_dict={'To Japan':'To_Japan', 'To Canada':'To_Canada', 'To Other Countries':'To_Other_Countries', 'Total Log Exports ':'Total_Log_Export'}
          
            check =  all([item in columns for item in ref])
           

        
        if not check or 'Total Log Exports' not in columns:
            raise IndexError
        
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
        prod_df=format_data(prod_df,prod_df.columns.tolist())
        prod_df.rename(columns=rename_dict,inplace=True)  
        
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","Lumber",filename+".csv",LogExportUS)

    else:
        raise IndexError







def abstract_lumber_exports_canada(tables,f,save_in_file="lumber_export_canada",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_lumber_exports_canada(tables,pathname=f_name,filename=save_in_file,date_index=date_index)

    except IndexError as e:
        try:
             process_lumber_exports_canada(tables,pathname=f_name,filename=save_in_file,date_index=date_index,table_index=1)


        except IndexError as e:
            try:
                tables=read_pdf(str(f),pages="2-3",area=(47, 0,54, 100),stream=True,relative_area=True)
                table_to_csv(tables)
                process_lumber_exports_canada(tables,pathname=f_name,filename=save_in_file,date_index=date_index,table_index=0)


            except Exception as e:

                try:
                    process_lumber_exports_canada(tables,pathname=f_name,filename=save_in_file,date_index=date_index,table_index=1)

                
                except IndexError as e:
                    
                     try:
                        tables=read_pdf(str(f),pages="2-3",area=(48, 0,55, 100),stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_lumber_exports_canada(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                    
                     except IndexError as e:
                         try:
                             tables=read_pdf(str(f),pages="2-3",area=(56, 0,63, 100),stream=True,relative_area=True)
                             table_to_csv(tables)
                             process_lumber_exports_canada(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                         except Exception as e:
                            try:
                                process_lumber_exports_canada(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)

                            except IndexError as e:

                                try:
                                     tables=read_pdf(str(f),pages="2-3",area=(56, 0,62, 100),stream=True,relative_area=True)
                                     table_to_csv(tables)
                                     process_lumber_exports_canada(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                                except Exception as e:
                                    try:
                                        process_lumber_exports_canada(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
                                    
                                    except Exception as e:
                                        try:
                                            tables=read_pdf(str(f),pages="3",area=(30, 0,35, 100),stream=True,relative_area=True)
                                            table_to_csv(tables)
                                            process_lumber_exports_canada(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                                        except IndexError as e:
                                           lumber_handler(f,
                                           f_name,save_in_file,date_index)
                                          
                                    

def lumber_handler(f,f_name,save_in_file,date_index):
    try:
        tables=read_pdf(str(f),pages="2-3",area=(45, 0,51, 100),stream=True,relative_area=True)
        table_to_csv(tables)
        process_lumber_exports_canada(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)

    except IndexError as e:
        try:
            process_lumber_exports_canada(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)

        except IndexError as e:
            raise e



                               



                    
                    
                        
                             #  process_logs_exports(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
                

                                                    

#process table one
def process_lumber_exports_canada(tables,pathname,date_index,table_index=0,filename="lumber_exports_canada"):
    """
     Description
    -----------
     function processes the us us log export data 
     
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


    
    if table.shape[1] in [13,14,15,16,17,18,19]:
        table=table.dropna(thresh=table.shape[1]-5, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        table.reset_index(inplace=True)
        table=table.drop_duplicates()
        prod_df=table.iloc[:,[1,6]]
        colmns=prod_df.columns.values
        
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)
        columns=prod_df.iloc[0].values
      
        
        if 'Unnamed: 0' in columns: 
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values


        if len(columns) == 5:
            ref=['To U.S.', 'To China' ,'To Japan', 'Other' ,'Total Lumber Exports']
            rename_dict={'To China':'To_China','To Japan':'To_Japan', 'To U.S.':'To_US', 'Total Lumber Exports':'Total_Lumber_Exports'}
            check =  all([item in columns for item in ref])
            filename=filename +"_1"

        else:
            ref=['To U.S.','To Japan','Other','Total Lumber Exports']
            rename_dict={'To Japan':'To_Japan', 'To U.S.':'To_US', 'Total Lumber Exports':'Total_Lumber_Exports'}
            check =  all([item in columns for item in ref])
           

        
        if not check or 'Total Lumber Exports' not in columns:
           
            raise IndexError
        
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
        prod_df=format_data(prod_df,prod_df.columns.tolist())
        prod_df.rename(columns=rename_dict,inplace=True)
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","Lumber",filename+".csv",LumberExportCanada)

    else:
        raise IndexError








def abstract_lumber_consumption_us(tables,f,save_in_file="lumber_consumption_us",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_lumber_consumption_us(tables,pathname=f_name,filename=save_in_file,date_index=date_index)

    except IndexError as e:
        try:
            process_lumber_consumption_us(tables,pathname=f_name,filename=save_in_file,date_index=date_index,table_index=1)


        except IndexError as e:
            
            try:
                tables=read_pdf(str(f),pages="2-3",area=(60, 0,67, 100),stream=True,relative_area=True)
                table_to_csv(tables)
                process_lumber_consumption_us(tables,pathname=f_name,filename=save_in_file,date_index=date_index,table_index=0)


            except Exception as e:
               

                try:
                    process_lumber_consumption_us(tables,pathname=f_name,filename=save_in_file,date_index=date_index,table_index=1)

                
                except IndexError as e:
                
                    
                     try:
                        tables=read_pdf(str(f),pages="2-3",area=(68, 0,75, 100),stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_lumber_consumption_us(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                    
                     except IndexError as e:
                        try:
                           process_lumber_consumption_us(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)

                        except IndexError as e:
                      
                    
                         try:
                             tables=read_pdf(str(f),pages="2-3",area=(68, 0,74, 100),stream=True,relative_area=True)
                             table_to_csv(tables)
                             process_lumber_consumption_us(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                         except Exception as e:
                              
                            try:
                                process_lumber_consumption_us(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)

                            except IndexError as e:
                                

                                try:
                                     tables=read_pdf(str(f),pages="3",area=(42, 0,48, 100),stream=True,relative_area=True)
                                     table_to_csv(tables)
                                     process_lumber_consumption_us(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                                except Exception as e:
                                    pass
                                    
                    
#process table one
def process_lumber_consumption_us(tables,pathname,date_index,table_index=0,filename="lumber_consumption_us"):
    """
     Description
    -----------
     function processes the us us log export data 
     
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
    if table.shape[1] in [13,14,15,16,17,18,19]:
        table=table.dropna(thresh=table.shape[1]-5, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        table.reset_index(inplace=True)
        table=table.drop_duplicates()

        prod_df=table.iloc[:,[1,6]]
        colmns=prod_df.columns.values
        
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)
        columns=prod_df.iloc[0].values
        
        if 'Unnamed: 0' in columns: 
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values

        ref=['Lumber Shipments','Plus Imports' ,'Minus Exports','Apparent Consumption']
        rename_dict={'Lumber Shipments':'Lumber_Shipments','Plus Imports':'Plus_Imports' ,'Minus Exports':'Minus_Exports','Apparent Consumption':'Apparent_Consumption'}
        
        check =  all([item in columns for item in ref])

        if not check:
            raise IndexError
        
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
        prod_df=format_data(prod_df,prod_df.columns.tolist())
       
        prod_df.rename(columns=rename_dict,inplace=True)
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","Lumber",filename+".csv",ConsumptionLumberUs)

    else:
        raise IndexError

















def abstract_lumber_consumption_canada(tables,f,save_in_file="lumber_consumption_canada",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_lumber_consumption_canada(tables,pathname=f_name,filename=save_in_file,date_index=date_index)

    except IndexError as e:
        
        
        try:
            tables=read_pdf(str(f),pages="2-3",area=(68, 0,74, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_lumber_consumption_canada(tables,pathname=f_name,filename=save_in_file,date_index=date_index,table_index=0)
        except IndexError as e:
            try:
                process_lumber_consumption_canada(tables,pathname=f_name,filename=save_in_file,date_index=date_index,table_index=0)
            
            
            except IndexError as e:
                try:
                    tables=read_pdf(str(f),pages="2-3",area=(70, 0,76, 100),stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_lumber_consumption_canada(tables,pathname=f_name,filename=save_in_file,date_index=date_index,table_index=0)
                except IndexError as e:
                    try:
                        process_lumber_consumption_canada(tables,pathname=f_name,filename=save_in_file,date_index=date_index,table_index=0)

                    except IndexError as e:
                
            
                        try:
                            tables=read_pdf(str(f),pages="2-3",area=(69, 0,77, 100),stream=True,relative_area=True)
                            table_to_csv(tables)
                            process_lumber_consumption_canada(tables,pathname=f_name,filename=save_in_file,date_index=date_index,table_index=0)


                        except Exception as e:
                            try:
                                process_lumber_consumption_canada(tables,pathname=f_name,filename=save_in_file,date_index=date_index,table_index=1)

                            except IndexError as e:
                                try:
                                    tables=read_pdf(str(f),pages="2-3",area=(70, 0,76, 100),stream=True,relative_area=True)
                                    table_to_csv(tables)
                                    process_lumber_consumption_canada(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                                
                                except IndexError as e:
                                    
                                    try:
                                        process_lumber_consumption_canada(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)

                                    except IndexError as e:
                                            handler_consumption(f,f_name,save_in_file,date_index)
                                        
                                     
                                            




                                             


def handler_consumption(f,f_name,save_in_file,date_index):
    try:
        tables=read_pdf(str(f),pages="2-3",area=(68, 0,77, 100),stream=True,relative_area=True)
        table_to_csv(tables)
        process_lumber_consumption_canada(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
    except IndexError as e:
        try:
            process_lumber_consumption_canada(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
        
        except IndexError as e:
            try:
                tables=read_pdf(str(f),pages="2-3",area=(78, 0,84, 100),stream=True,relative_area=True)
                table_to_csv(tables)
                process_lumber_consumption_canada(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
            except IndexError as e:
                try:
                    process_lumber_consumption_canada(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)
                except IndexError as e:
                    try:
                        tables=read_pdf(str(f),pages="2-3",area=(79, 0,84, 100),stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_lumber_consumption_canada(tables,pathname=f_name,table_index=0,filename=save_in_file,date_index=date_index)
                    except Exception as e:
                        try:
                            process_lumber_consumption_canada(tables,pathname=f_name,table_index=1,filename=save_in_file,date_index=date_index)

                        except Exception as e:
                            pass
                     
                   

                        
               
                

           

      
   
    
                                







                    
#process table one
def process_lumber_consumption_canada(tables,pathname,date_index,table_index=0,filename="lumber_consumption_canada"):
    """
     Description
    -----------
     function processes the us us log export data 
     
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
    if table.shape[1] in [13,14,15,16,17,18,19]:
        table=table.dropna(thresh=table.shape[1]-5, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        table.reset_index(inplace=True)
        table=table.drop_duplicates()

        if table.shape[1] == 13:
            prod_df=table.iloc[:,[1,5]]
        

        else:
            prod_df=table.iloc[:,[1,6]]
        
        
        colmns=prod_df.columns.values
        

        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)
        columns=prod_df.iloc[0].values
        
        if 'Unnamed: 0' in columns: 
            raise IndexError

        ref=['Lumber Shipments','Plus Imports' ,'Minus Exports','Apparent Consumption']
        rename_dict={'Lumber Shipments':'Lumber_Shipments','Plus Imports':'Plus_Imports' ,'Minus Exports':'Minus_Exports','Apparent Consumption':'Apparent_Consumption'}
        check =  all([item in columns for item in ref])

        if not check:
            raise IndexError
        
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]
        prod_df=format_data(prod_df,prod_df.columns.tolist())
        prod_df.rename(columns=rename_dict,inplace=True)
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","Lumber",filename+".csv",ConsumptionLumberCanada)



    else:
        raise IndexError






def abstract_north_american_production(tables,f,save_in_file="north_american_production_estimate",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=NorthAmericanProduction)

    except IndexError as e:
        try:
            tables=read_pdf(str(f),pages="3",area=(11, 0,22, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=NorthAmericanProduction)

        except Exception as e:
            try:
                tables=read_pdf(str(f),pages="3",area=(14, 0,29, 100),stream=True,relative_area=True)
                table_to_csv(tables)
                process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=NorthAmericanProduction)
            except Exception as e:
                pass


           
    
        
        


                    
#process table one
def process_north_american(tables,pathname,date_index,table_index=0,filename="north_american_production_estimate",exclude_canada=False,mapper=None):
    """
     Description
    -----------
     function processes the us us log export data 
     
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
    if table.shape[1] in [13,14,15,16,17,18,19]:
        table=table.dropna(thresh=table.shape[1]-5, axis=0)
        table=table.dropna(thresh=table.shape[0]-3, axis=1)
        table.reset_index(inplace=True)
        table=table.drop_duplicates()
        prod_df=table.iloc[:,[1,-1]]
        prod_df=prod_df.transpose()
        prod_df.reset_index(inplace=True)
        columns=prod_df.iloc[0].values

 
        if 'Unnamed: 0' in columns:
               
            prod_df=prod_df.iloc[:,1:]
            columns=prod_df.iloc[0].values 
        
        if 'Region' in columns: 
            prod_df=prod_df.iloc[:,1:]

            columns=prod_df.iloc[0].values

        if exclude_canada == False:
            
            if 'Cal. Coast' in columns:
                ref=['Coast' ,'Inland' ,'Cal. Coast' ,'Total West','South' ,'Other','Total U.S.','British Columbia','Prairies & Eastern Canada', 'Total Canada','Total North America']
                rename_dict={'Total U.S.':'Total_US','Cal. Coast':'Cal_Coast','British Columbia':'British_Columbia','Prairies & Eastern Canada':'Prairies_And_Eastern Canada', 'Total Canada':'Total_Canada',"Total North America":"Total North America",'Total West':'Total_West'}
            
            else:
                ref=['Coast' ,'Inland' ,'Cal. Redwood' ,'Total West','South' ,'Other','Total U.S.','British Columbia','Prairies & Eastern Canada', 'Total Canada','Total North America']
                rename_dict={'Total U.S.':'Total_US','Cal. Redwood':'Cal_Redwood','British Columbia':'British_Columbia','Prairies & Eastern Canada':'Prairies_And_Eastern Canada', 'Total Canada':'Total_Canada',"Total North America":"Total North America",'Total West':'Total_West'}



        else:
        
            if 'Cal. Coast' in columns:
                 ref=['Coast', 'Inland' ,'Cal. Coast', 'Total West', 'South', 'Other' ,'Total U.S.']
                 rename_dict={'Total U.S.':'Total_US','Cal. Coast':'Cal_Coast', 'Total West':'Total_West',}
            

            
            else:
                ref=['Coast', 'Inland' ,'Cal. Redwood', 'Total West', 'South', 'Other' ,'Total U.S.']
                rename_dict={'Total U.S.':'Total_US','Cal. Redwood':'Cal_Redwood','Total West':'Total_West'}

            

        
        prod_df.columns=prod_df.iloc[0].values
        prod_df=prod_df[1:]        
        check =  all([item in columns for item in ref])



           



        if not check:
            raise IndexError


        
        
      
     
        prod_df=format_data(prod_df,prod_df.columns)
        prod_df.rename(columns=rename_dict,inplace=True)
        prod_df["date"]=date_index
        prod_df["File"]=pathname
        save_to_dir(prod_df,"DATA","Lumber",filename+".csv",mapper)

    else:
        raise IndexError






def abstract_north_american_shipment(tables,f,save_in_file="north_american_shipment_estimate",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=NorthAmericanShipment)

    except IndexError as e:

        try:
            tables=read_pdf(str(f),pages="3",area=(23, 0,35, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=NorthAmericanShipment)

        except Exception as e:
            try:
           
                tables=read_pdf(str(f),pages="3",area=(33, 0,49, 100),stream=True,relative_area=True)
                table_to_csv(tables)
                process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=NorthAmericanShipment)

            except Exception as e:
                try:
                    tables=read_pdf(str(f),pages="4",area=(33, 0,49, 100),stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index)
                except Exception as e:
                    pass

          


           
    
        
        


                    
#



def abstract_north_american_orders(tables,f,save_in_file="north_american_orders_estimate",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,exclude_canada=True,mapper=NorthAmericanOrder)

    except IndexError as e:
   

        try:
            tables=read_pdf(str(f),pages="3",area=(37, 0,42, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,exclude_canada=True,mapper=NorthAmericanOrder)

        except Exception as e:
            
            try:
                tables=read_pdf(str(f),pages="3",area=(54, 0,63, 100),stream=True,relative_area=True)
                table_to_csv(tables)
                process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,exclude_canada=True,mapper=NorthAmericanOrder)
            except Exception as e:
             
             

            
                
                try:
                    tables=read_pdf(str(f),pages="4",area=(37, 0,42, 100),stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,exclude_canada=True,mapper=NorthAmericanOrder)
                except Exception as e:
                    pass




                    
#



def abstract_north_american_unfilled_orders(tables,f,save_in_file="north_american_unfilled_orders_estimate",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,exclude_canada=True,mapper=NorthAmericanUnfiledOrder)

    except Exception as e:
        pass
   

        try:
            tables=read_pdf(str(f),pages="3",area=(46, 0,52, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,exclude_canada=True,mapper=NorthAmericanUnfiledOrder)

        except Exception as e:

            try:
                tables=read_pdf(str(f),pages="3",area=(67, 0,77, 100),stream=True,relative_area=True)
                table_to_csv(tables)
                process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,exclude_canada=True,mapper=NorthAmericanUnfiledOrder)




            except Exception as e:


                try:
                    tables=read_pdf(str(f),pages="3",area=(65, 0,74, 100),stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,exclude_canada=True,mapper=NorthAmericanUnfiledOrder)




                except Exception as e:

                    try:
 
                        tables=read_pdf(str(f),pages="4",area=(46, 0,52, 100),stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,exclude_canada=True,mapper=NorthAmericanUnfiledOrder)
                    except Exception as e:
                        try:
                            tables=read_pdf(str(f),pages="4",area=(65, 0,74, 100),stream=True,relative_area=True)
                            table_to_csv(tables)
                            process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,exclude_canada=True,mapper=NorthAmericanUnfiledOrder)
                        except Exception as e:
                            pass
                    

                    




def abstract_north_american_inventory(tables,f,save_in_file="north_american_inventory",date_index=None):
    """
    PARAMETERS
    -------------------------------------

    tables : list list of tables abstracted

    f_name : file path
    
    Returns:
    --------

    date:datetime 
    
    """
        
    f_name= str(f)
        
    
    # data is scattered along first second and third so exceptions are handled to traverse through them
    try:
        process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=NorthAmericanInventory)

    except Exception as e:

   

        try:
            tables=read_pdf(str(f),pages="3",area=(53, 0,65, 100),stream=True,relative_area=True)
            table_to_csv(tables)
            process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=NorthAmericanInventory)

        except Exception as e:


            try:
                tables=read_pdf(str(f),pages="3",area=(80, 0,94, 100),stream=True,relative_area=True)
                table_to_csv(tables)
                process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=NorthAmericanInventory)


            except Exception as e:
           

                try:
                    tables=read_pdf(str(f),pages="3",area=(77, 0,94, 100),stream=True,relative_area=True)
                    table_to_csv(tables)
                    process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=NorthAmericanInventory)



                except Exception as e:

                    try:
           
                        tables=read_pdf(str(f),pages="4",area=(53, 0,65, 100),stream=True,relative_area=True)
                        table_to_csv(tables)
                        process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,mapper=NorthAmericanInventory)

                    except Exception as e:
                        try:
                            tables=read_pdf(str(f),pages="4",area=(77, 0,94, 100),stream=True,relative_area=True)
                            table_to_csv(tables)
                            process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,exclude_canada=False,mapper=NorthAmericanInventory)
                        except Exception as e:
                            try:
                                tables=read_pdf(str(f),pages="4",area=(80, 0,94, 100),stream=True,relative_area=True)
                                table_to_csv(tables)
                                process_north_american(tables,pathname=f_name,filename=save_in_file,date_index=date_index,exclude_canada=False,mapper=NorthAmericanInventory)
                            except Exception as e:
                                pass






# #paths to the pdfs
# lumber_path="F:\\Traders\\2x4\\2x4 v2\\Data\\Fundamentals\\Historical Data 10 Years\\Historical Data 10 Years\\Lumber Track"
# st=time.time()
# pdfs=fetch_files(PDF_DIR=lumber_path)
# abstract_data(pdfs,save_in_file="export_us",date_only=False)
# pdfs=fetch_files(dir_name="WWPA",sub_dir_name="Barometer")
# # abstract_barometer_data(pdfs)
# print("time taken to completely abstract data is {} secs".format(time.time()-st))

