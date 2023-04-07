
import pandas as pd
import numpy as np
from pyrsistent import v
from sqlalchemy import column
from lumberdataabstract import format_data
from tabula import read_pdf
import os
from os import walk
import pandas as pd
import numpy as np
from datetime import datetime
from backend.db.models import *
from common import save_to_dir,fetch_files,check_if_exists






def get_pdf_date(filename):
   
    pre_date_text = ['BAROMETER For the week ending:','For the week ending:','For the week ending:Days']
    flag = False
    date_ = None
    try:
       
        dfs = read_pdf(filename, pages = 'all', guess = False)

        for df in dfs:
            for col in df.columns:
                for text in pre_date_text:
                   
                    if text in col:
                        date_string = (col.split(text)[1])
                        date_ = datetime.strptime(date_string, '%B %d, %Y').date()
                       
                        flag = True
                        break # Break after you've found 1st instance of date
                                           
                if flag:
                    break # Break after you've found 1st instance of date

            if flag:
                break # Break after you've found 1st instance of date
    except:
        print('No date found in ', filename)
       

    return date_

def get_fundamental_table(date_, file):

        cols = ['Production', 'Orders', 'Shipments', 'Unfilled', 'Inventories', 'File']


        tables = read_pdf(file,multiple_tables=True,pages='all',encoding='utf-8',stream=True, guess=True)

        for table in tables:
            if 'Reporting Week' in table.columns:
                final_table = table
       
        df = pd.DataFrame()
        for col in final_table:
            df = pd.concat([df,final_table[col].str.split(" ", expand=True)], axis=1)


        df = df.fillna(value=np.nan)
        df.columns = [i for i,j in enumerate(df.columns)]
        df.index = df.iloc[:,0]
        df.drop([0, 1], axis=1)
        df = df[[2]]

        df_idx = list(df.index)
        idx_west = df_idx.index('Western')
        idx_coast = df_idx.index('Coast')
        idx_inland = df_idx.index('Inland')

        iter_df = pd.DataFrame(columns=cols, index = [date_])
        iter_df1 = pd.DataFrame(columns=cols, index = [date_])
        iter_df2 = pd.DataFrame(columns=cols, index = [date_])

        mini_df =df.iloc[idx_west+1:idx_coast,0]
        mini_df1 = df.iloc[idx_coast+1:idx_inland,0]
        mini_df2 = df.iloc[idx_inland+1:,0]

        for col in cols:
           
            if col == 'File':
                iter_df[col] = str(file)
                iter_df1[col] = str(file)
                iter_df2[col] = str(file)
               
            else:
                iter_df[col] = mini_df[col]
                iter_df1[col] = mini_df1[col]
                iter_df2[col] = mini_df2[col]

        return iter_df, iter_df1, iter_df2



def get_finishedinventory_table(date_, file):

        cols1 = ['Coast Region', 'Inland Region', 'Western Totals', 'File']


        tables = read_pdf(file,multiple_tables=True,pages='all',encoding='utf-8',stream=True, guess=True)

        for table in tables:
            if 'Finished Inventory /' in table.columns:
                final_table = table
       
        df = pd.DataFrame()
        for col in final_table:
            df = pd.concat([df,final_table[col].str.split(" ", expand=True)], axis=1)

        df = df.fillna(value=np.nan)
        df = df.replace('%',np.nan)
        df = df.dropna(how='all', axis=1)
        df = df[2].dropna(how='all')

        iter_df = pd.DataFrame(columns=cols1, index = [date_])

        iter_df['File'] = file
        iter_df['Coast Region'] = df.iloc[0]
        iter_df['Inland Region'] = df.iloc[1]
        iter_df['Western Totals'] = df.iloc[2]

        return iter_df




def wwpa_barometer_tables(filename_dict,check_path):


    file_errors = []
    cols = ['Production', 'Orders', 'Shipments', 'Unfilled', 'Inventories', 'File']
    cols1 = ['Coast Region', 'Inland Region', 'Western Totals', 'File']

    western_df = pd.DataFrame(columns=cols)
    coast_df = pd.DataFrame(columns=cols)
    inland_df = pd.DataFrame(columns=cols)
    finished_inv = pd.DataFrame(columns=cols1)


    dates_list = []

    for file in filename_dict:
        check=check_if_exists(check_path,file,relative=False)
      
        if check:
            print("record already exists")
            continue
        print(file)
        df = read_pdf(file, pages = 'all', guess = False)
       
        date_ = get_pdf_date(file)
       
        if date_:
           
            try:
                iter_df, iter_df1, iter_df2 = get_fundamental_table(date_, file)

                western_df = pd.concat([western_df, iter_df],axis=0)
                coast_df = pd.concat([coast_df, iter_df1],axis=0)
                inland_df = pd.concat([inland_df, iter_df2],axis=0)

                dates_list.append(date_)

               
            except:
                file_errors.append(file)
       
        else:
            file_errors.append(file)

    return western_df, coast_df, inland_df,dates_list, file_errors


# Errors only in following pdfs
# ['F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2010\\06 June\\Barometer adjustment letter 2010.pdf',
#  'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2011\\07 July\\Barometer revision notice.pdf',
#  'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2013\\01 January\\BAR130119R-FAX.pdf',
#  'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2014\\05 May\\Bar140503.pdf',
#  'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2014\\05 May\\Bar140510.pdf',
#  'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2015\\07 July\\Barometer revision notice 2015.pdf']

# NOT USING THIS ANYMORE
# ignore_list = ['F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2010\\06 June\\Bar100626.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2010\\06 June\\Barometer adjustment letter 2010.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2011\\07 July\\Bar110730rev.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2011\\07 July\\Barometer revision notice.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2012\\06 June\\Bar120630.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2013\\01 January\\Barometer-2ndPage-Rev.ps',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2013\\01 January\\BAR130119R-FAX.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2013\\07 July\\Bar130706.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2014\\05 May\\Bar140503.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2014\\05 May\\Bar140510.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2014\\05 May\\Bar140705.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2014\\07 July\\Bar140705.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2014\\07 July\\Thumbs.db',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2014\\08 August\\Revised Barometer Fax Letter.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2015\\07 July\\Barometer revision notice 2015.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2015\\07 July\\Bar150711.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2016\\07 July\\Bar160702.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2017\\07 July\\Bar170708.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2018\\07 July\\Bar180714.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2019\\07 July\\Bar190706.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2020\\08 August\\Bar200808.pdf',
# 'F:/Traders/2x4/2x4 v2/Data/Fundamentals/Historical Data 10 Years/Historical Data 10 Years/Barometer/2021\\07 July\\Bar210731.pdf']

def abstract_data(filename_list,check_path):
    western_df, coast_df, inland_df, dates_list, file_errors = wwpa_barometer_tables(filename_list,check_path)

   
  

    western_df=western_df.drop_duplicates(keep='last',subset=['File'])
    coast_df=coast_df.drop_duplicates(subset=["File"],keep="last")
    inland_df=inland_df.drop_duplicates(subset=["File"],keep="last")
   
    western_df.reset_index(inplace=True)
    western_df.rename(columns={"index":"date"},inplace=True)
    western_df=format_data(western_df,western_df.columns.tolist())

    coast_df.reset_index(inplace=True)
    coast_df.rename(columns={"index":"date"},inplace=True)
    coast_df=format_data(coast_df,coast_df.columns.tolist())
    
    inland_df.reset_index(inplace=True)
    inland_df.rename(columns={"index":"date"},inplace=True)
    inland_df=format_data(inland_df,inland_df.columns.tolist())
    return western_df, coast_df, inland_df
    
    





    # save_to_dir(western_df,"DATA","Barometer","barometer_western.csv",process=True)
    # save_to_dir(inland_df,"DATA","Barometer","barometer_inlands.csv",process=True)
    # save_to_dir(coast_df,"DATA","Barometer","barometer_coast.csv",process=True)
    
    
# pdfs=fetch_files(dir_name="WWPA",sub_dir_name="Barometer")
# import pathlib
# check_path_barometer=pathlib.Path(__file__).resolve().parents[1]/'DATA'/'Barometer/barometer_coast.csv'
# abstract_data(pdfs,check_path_barometer)








