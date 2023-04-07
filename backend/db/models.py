from email.policy import default
import sys,os,pathlib
from psycopg2 import Timestamp
# we're appending the app directory to our path here so that we can import config easily
sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import  BigInteger, inspect,MetaData
from sqlalchemy import Column, Integer, String,DateTime,UniqueConstraint, ForeignKey, Boolean, Float, Enum,TIMESTAMP,Date
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, backref
from sqlalchemy.event import listens_for
import enum
from dotenv import load_dotenv


load_dotenv
meta_obj=MetaData(schema=os.environ.get("POSTGRES_SCHEMA"))
Base = declarative_base(metadata=meta_obj)


class ProductionUS(Base):

    #table_name
    __tablename__="production_us"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    West=Column("West",BigInteger)
    South=Column("South",BigInteger)
    Other=Column("Other",BigInteger)
    Total=Column("Total",BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs }

    


class ShipmentUS(Base):

    #table_name
    __tablename__="shipment_us"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    West=Column("West",BigInteger)
    South=Column("South",BigInteger)
    Other=Column("Other",BigInteger)
    Total=Column("Total",BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs }

   

class InventoryUS(Base):

    #table_name
    __tablename__="inventory_us"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    West=Column("West",BigInteger)
    South=Column("South",BigInteger)
    Other=Column("Other",BigInteger)
    Total=Column("Total",BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs }

 
 
class OrdersUS(Base):

    #table_name
    __tablename__="orders_us"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    West=Column("West",BigInteger)
    South=Column("South",BigInteger)
    Other=Column("Other",BigInteger)
    Total=Column("Total",BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs }


 
class PPPC(Base):

    #table_name
    __tablename__="pppc"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    West=Column("West",BigInteger)
    South=Column("South",BigInteger)
    Total_US=Column("Total_US",BigInteger)
    British_Columbia=Column("British_Columbia",BigInteger)
    East_of_the_Rockies=Column("East_of_the_Rockies",BigInteger)
    Total_Canada=Column("Total_Canada",BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs }


class UnfilledOrdersUS(Base):

    #table_name
    __tablename__="unfilledorders_us"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    West=Column("West",BigInteger)
    South=Column("South",BigInteger)
    Other=Column("Other",BigInteger)
    Total=Column("Total",BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs }




class ProductionCanada(Base):

    #table_name
    __tablename__="production_canada"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    British_Columbia=Column("British_Columbia",BigInteger)
    East_of_the_Rockies=Column("East_of_the_Rockies",BigInteger)
    Total=Column("Total",BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 



class ShipmentCanada(Base):

    #table_name
    __tablename__="shipment_canada"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    British_Columbia=Column("British_Columbia",BigInteger)
    East_of_the_Rockies=Column("East_of_the_Rockies",BigInteger)
    Total=Column("Total",BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 


class InventoryCanada(Base):

    #table_name
    __tablename__="inventory_canada"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    British_Columbia=Column("British_Columbia",BigInteger)
    East_of_the_Rockies=Column("East_of_the_Rockies",BigInteger)
    Total=Column("Total",BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 





class LumberExportCanada(Base):

    #table_name
    __tablename__="lumber_export_canada"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    To_US=Column("To_US",BigInteger)
    To_China=Column(" To_China",BigInteger,default=000)
    To_Japan=Column(" To_Japan",BigInteger)
    Total_Lumber_Exports=Column("Total_Lumber_Export",BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}


class LogExportUS(Base):

    #table_name
    __tablename__="log_export_us"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    To_Canada=Column("To_Canada",BigInteger)
    To_China=Column("To_China",BigInteger,default=000)
    To_Japan=Column("To_Japan",BigInteger)
    To_Other_Countries=Column("To_Other_Countries",BigInteger)
    
    Total_Log_Export=Column("Total_Log_Export",BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}



class LogImportsUS(Base):

    #table_name
    __tablename__="log_import_us"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    From_Canada=Column("From_Canada",BigInteger)
    Non_Canadian_Sources=Column("Non_Canadian_Sources",BigInteger,default=000)
    Total_Log_Imports=Column("Total_Log_Imports",BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}








class ConsumptionLumberUs(Base):

    #table_name
    __tablename__="consumption_lumber_us"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    Lumber_Shipments=Column("Lumber_Shipments",BigInteger)
    Plus_Imports=Column("Plus_Imports",BigInteger)
    Minus_Exports=Column("Minus_Exports",BigInteger)
    Apparent_Consumption=Column("Apparent_Consumption",BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 



class LumberExportUS(Base):

    #table_name
    __tablename__="lumber_export_us"
    

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    To_Canada=Column("To_Canada",BigInteger)
    To_China=Column("To_China",BigInteger,default=000)
    To_Japan=Column("To_Japan",BigInteger)
    To_Mexico =Column("'To_Mexico' ",BigInteger)
    To_Other_Countries=Column("To_Other_Countries",BigInteger)
    
    Total_Lumber_Export=Column("Total_Lumber_Export",BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}




class LumberImportsUS(Base):

    #table_name
    __tablename__="lumber_import_us"


    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    From_British_Columbia=Column("From_British_Columbia",BigInteger)
    To_China=Column("East_of_the_Rockies",BigInteger,default=000)
    From_Latin_America=Column('From_Latin_America',BigInteger)
    From_Europe=Column('From_Europe',BigInteger)
    Total_Non_Canadian=Column('Total_Non_Canadian',BigInteger)
    Total_Lumber_Imports=Column('Total_Lumber_Imports',BigInteger)
    
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}


















class ConsumptionLumberCanada(Base):

    #table_name
    __tablename__="consumption_lumber_canada"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    Lumber_Shipments=Column("Lumber_Shipments",BigInteger)
    Plus_Imports=Column("Plus_Imports",BigInteger)
    Minus_Exports=Column("Minus_Exports",BigInteger)
    Apparent_Consumption=Column("Apparent_Consumption",BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 




class NorthAmericanProduction(Base):
    
            

    #table_name
    __tablename__="north_american_production"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    Coast=Column("Coast",BigInteger)
    Inland=Column("Inland",BigInteger)
    Cal_Coast=Column("Cal_Coast",BigInteger,default=000)
    Cal_Redwood=Column('Cal_Redwood',BigInteger,default=000)
    South=Column('South',BigInteger)
    Other=Column("Other",BigInteger)
    Total_US=Column("Total_US",BigInteger)
    British_Columbia=Column('British Columbia',BigInteger)
    Prairies_And_Eastern_Canada=Column('Prairies_And_Eastern_Canada',BigInteger)
    Total_Canada=Column('Total_Canada',BigInteger)
    Total_North_America=Column('Total_North_America',BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 





class NorthAmericanShipment(Base):
    
            

    #table_name
    __tablename__="north_american_shipment"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    Coast=Column("Coast",BigInteger)
    Inland=Column("Inland",BigInteger)
    Cal_Coast=Column("Cal_Coast",BigInteger,default=000)
    Cal_Redwood=Column('Cal_Redwood',BigInteger,default=000)
    South=Column('South',BigInteger)
    Other=Column("Other",BigInteger)
    Total_US=Column("Total_US",BigInteger)
    British_Columbia=Column('British Columbia',BigInteger)
    Prairies_And_Eastern_Canada=Column('Prairies_And_Eastern_Canada',BigInteger)
    Total_Canada=Column('Total_Canada',BigInteger)
    Total_North_America=Column('Total_North_America',BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 


class NorthAmericanOrder(Base):
    
            

    #table_name
    __tablename__="north_american_order"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    Coast=Column("Coast",BigInteger)
    Inland=Column("Inland",BigInteger)
    Cal_Coast=Column("Cal_Coast",BigInteger,default=000)
    Cal_Redwood=Column('Cal_Redwood',BigInteger,default=000)
    Total_West=Column('Total_West',BigInteger,default=000)
    South=Column('South',BigInteger)
    Other=Column("Other",BigInteger)
    Total_US=Column("Total_US",BigInteger)
   
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 


class NorthAmericanUnfiledOrder(Base):
    
            

    #table_name
    __tablename__="north_american_unfilled_order"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    Coast=Column("Coast",BigInteger)
    Inland=Column("Inland",BigInteger)
    Cal_Coast=Column("Cal_Coast",BigInteger,default=000)
    Cal_Redwood=Column('Cal_Redwood',BigInteger,default=000)
    Total_West=Column('Total_West',BigInteger,default=000)
    South=Column('South',BigInteger)
    Other=Column("Other",BigInteger)
    Total_US=Column("Total_US",BigInteger)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 



class NorthAmericanInventory(Base):
    
    #table_name
    __tablename__="north_american_inventory"

    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    Coast=Column("Coast",BigInteger)
    Inland=Column("Inland",BigInteger)
    Cal_Coast=Column("Cal_Coast",BigInteger,default=000)
    Cal_Redwood=Column('Cal_Redwood',BigInteger,default=000)
    Total_West=Column('Total_West',BigInteger,default=000)
    South=Column('South',BigInteger)
    Other=Column("Other",BigInteger)
    Total_US=Column("Total_US",BigInteger)

    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 

class OrdersWestern(Base):
    
    #table_name
    __tablename__="orders_western"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    
    #columns
    Coast=Column("Coast",BigInteger)
    Inland=Column("Inland",BigInteger)
    Cal_RW=Column("Cal_RW",BigInteger,default=000)
    Western_Total=Column('Western_Total',BigInteger,default=000)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 


class PPPCWestern(Base):
    
    #table_name
    __tablename__="pppc_western"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    
    #columns
    Coast=Column("Coast",BigInteger)
    Inland=Column("Inland",BigInteger)
    Cal_RW=Column("Cal_RW",BigInteger,default=000)
    Western_Total=Column('Western_Total',BigInteger,default=000)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 




class ProductionWestern(Base):
    
    #table_name
    __tablename__="production_western"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    
    #columns
    Coast=Column("Coast",BigInteger)
    Inland=Column("Inland",BigInteger)
    Cal_RW=Column("Cal_RW",BigInteger,default=000)
    Western_Total=Column('Western_Total',BigInteger,default=000)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    Day=Column("Day",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 


class BarometerWestern(Base):
    
    #table_name
    __tablename__="barometer_western"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    
    #columns
    Production=Column("Production",BigInteger)
    Orders=Column("Orders",BigInteger)
    Shipments=Column("Shipments",BigInteger,default=000)
    Unfilled=Column('Unfilled',BigInteger,default=000)
    Inventories=Column('Inventories',BigInteger,default=000)
    Year=Column("Year",BigInteger)
    Month=Column("Month",BigInteger)
    Day=Column("Day",BigInteger)
    File=Column("File",String,primary_key=True)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 


class BarometerCoast(Base):
    
    #table_name
    __tablename__="barometer_coast"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    
    #columns
    Production=Column("Production",BigInteger)
    Orders=Column("Orders",BigInteger)
    Shipments=Column("Shipments",BigInteger,default=000)
    Unfilled=Column('Unfilled',BigInteger,default=000)
    Inventories=Column('Inventories',BigInteger,default=000)
    Year=Column("Year",BigInteger)
    Month=Column("Month",BigInteger)
    Day=Column("Day",BigInteger)
    File=Column("File",String,primary_key=True)
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 


class BarometerInland(Base):
    
    #table_name
    __tablename__="barometer_inland"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    
    #columns
    Production=Column("Production",BigInteger)
    Orders=Column("Orders",BigInteger)
    Shipments=Column("Shipments",BigInteger,default=000)
    Unfilled=Column('Unfilled',BigInteger,default=000)
    Inventories=Column('Inventories',BigInteger,default=000)
    Year=Column("Year",BigInteger)
    Month=Column("Month",BigInteger)
    Day=Column("Day",BigInteger)
    File=Column("File",String,primary_key=True)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 


class BarometerFinishedInventories(Base):
    
    #table_name
    __tablename__="barometer_finished_inventories"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)

    #columns
    Coast_Region=Column("Coast_Region",Float,primary_key=True)
    Inland_Region=Column("Inland_Region",Float,primary_key=True)
    Western_Totals=Column("Western_Totals",Float,default=000)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    Day=Column("Day",BigInteger,primary_key=True)
    File=Column("File",String,primary_key=True)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 



class UnfilledOrderWestern(Base):
    
    #table_name
    __tablename__="unfilled_order_western"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    
    #columns
    Coast=Column("Coast",BigInteger,primary_key=True)
    Inland=Column("Inland",BigInteger,primary_key=True)
    Cal_RW=Column("Cal_RW",BigInteger,default=000)
    Western_Total=Column('Western_Total',BigInteger,default=000)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 



class ShipmentWestern(Base):
    
    #table_name
    __tablename__="shipment_western"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    
    #columns
    Coast=Column("Coast",BigInteger,primary_key=True)
    Inland=Column("Inland",BigInteger)
    Cal_RW=Column("Cal_RW",BigInteger,default=000)
    Western_Total=Column('Western_Total',BigInteger,default=000)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 



class InventoryWestern(Base):
    
    #table_name
    __tablename__="inventory_western"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    
    #columns
    Coast=Column("Coast",BigInteger)
    Inland=Column("Inland",BigInteger)
    Cal_RW=Column("Cal_RW",BigInteger,default=000)
    Western_Total=Column('Western_Total',BigInteger,default=000)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 


class ShipmentCoastal(Base):
    
    #table_name
    __tablename__="shipment_coastal"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    Northeast=Column("Northeast",BigInteger)
    Midwest=Column("Midwest",BigInteger)
    South=Column("South",BigInteger)
    West=Column('West',BigInteger,default=000)
    Total=Column('Total',BigInteger,default=000)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 


class ShipmentInland(Base):
    
    #table_name
    __tablename__="shipment_inland"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    #columns
    Northeast=Column("Northeast",BigInteger)
    Midwest=Column("Midwest",BigInteger)
    South=Column("South",BigInteger)
    West=Column('West',BigInteger,default=000)
    Total=Column('Total',BigInteger,default=000)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 



class AveragePriceCoastal(Base):
    
    #table_name
    __tablename__="average_price_coastal"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)

    
    #columns
    Douglas_Fir_Dry=Column("Douglas_Fir_Dry",Float)
    Douglas_Fir_Green=Column("Douglas_Fir_Green",Float)
    Douglas_Fir_All=Column("Douglas_Fir_All",Float)
    Hem_Fir_Dry=Column('Hem_Fir_Dry',Float)
    Hem_Fir_Green=Column('Hem_Fir_Green',Float)
    Hem_Fir_All=Column('Hem_Fir_All',Float)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 


class AveragePriceIsland(Base):
   
    #table_name
    __tablename__="average_price_island"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)\

    #columns
    Ponderosa_Pine=Column("Ponderosa_Pine",Float)
    Douglas_Fir_and_Larch_Dry=Column("Douglas_Fir_and_Larch_Dry",Float)
    Douglas_Fir_and_Larch_Green=Column("Douglas_Fir_and_Larch_Green",Float)
    White_Fir=Column('White_Fir',Float)
    Englemann_Spruce=Column('Englemann_Spruce',Float)
    Western_Red_Cedar=Column('Western_Red_Cedar',Float)
    Whitewoods=Column('Whitewoods',Float)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs} 


class AveragePriceCostalDoglas(Base):
    
    #table_name
    __tablename__="average_price_coastal_douglus"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)

    #columns
    Stand_and_Btr_RL_2x4=Column("Stand_and_Btr_RL_2x4",Float)
    Stud_Stand_and_Btr_8_2x4=Column("Stud_Stand_and_Btr_8_2x4",Float)
    No2_and_Btr_RL_2x10=Column("No2_and_Btr_RL_2x10",Float)
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}

class AveragePriceCostalHamfir(Base):
    
    #table_name
    __tablename__="average_price_coastal_himfir"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
    
    #columns
    Stand_and_Btr_RL_2x4=Column("Stand_and_Btr_RL_2x4",Float)
    No2_and_Btr_RL_2x10=Column("No2_and_Btr_RL_2x10",Float)
    
    
    
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}


class AveragePriceIslandDouglas(Base):
    
    #table_name
    __tablename__="average_price_island_douglas"
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)


    
    #columns
    Stud_8_2x4=Column("Stud_8_2x4",Float)
    No2_and_Btr_RL_2x10=Column("No2_and_Btr_RL_2x10",Float)
    
    
    
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}


class AveragePricePonderasoPine(Base):
    
    #table_name
    __tablename__="average_price_ponderaso_pine"
               
            
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)


    
    #columns
    Moulding_and_Btr_RW_5_4=Column("Moulding_and_Btr_RW_5_4",Float)
    No_2_Shop_RW_5_4=Column("No_2_Shop_RW_5_4",Float)
    No_3_Shop_RW_5_4=Column("No_3_Shop_RW_5_4",Float)
    No_2_and_Btr_Com_RL_1x6=Column("No_2_and_Btr_Com_RL_1x6",Float)
   
    
    
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}




class AveragePriceWhiteWoods(Base):
    
    #table_name
    __tablename__="average_price_white_woods"
               
            
    
    #primary
    Timestamp=Column("Timestamp",Date,primary_key=True)
   
    #columns
    Stud_8_2x4=Column("Stud_8_2x4",Float)
    Stud_8_2x6=Column("Stud_8_2x6",Float)
   
    
    
    Year=Column("Year",BigInteger,primary_key=True)
    Month=Column("Month",BigInteger,primary_key=True)
    File=Column("File",String)
    
    def toDict(self):
        return { c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}