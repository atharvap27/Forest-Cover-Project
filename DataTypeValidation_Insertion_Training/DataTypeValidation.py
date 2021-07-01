import sqlite3 as sql
from app_logger.App_Logger import App_logger
import os
from datetime import datetime
import csv
import shutil
from os import listdir

class DataBaseConnection():
    def __init__(self):
        self.logger=App_logger()
        self.path='Training_database/'
        self.badFilePath='TrainingSet_Rawfiles_after_validation/BadData'
        self.goodFilePath='TrainingSet_Rawfiles_after_validation/GoodData'

    def dbConnection(self,db_name):
        try:
            conn=sql.connect(self.path+db_name+'.db')
            f=open('Training_logs/dbConnectionLog.txt','a++')
            self.logger.log(f,'Opened %s database successfully'%db_name)
            f.close()
        except ConnectionError as c:
            f=open('Training_logs/dbConnectionLog.txt','a++')
            self.logger.log(f,'Error in connecting to database:%s'%c)
            f.close()
            raise c
        return conn

    def createTableIntoDatabase(self,db_name,column_names):
        try:
            conn=self.dbConnection(db_name)
            c=conn.cursor()
            c.execute('SELECT count(name) FROM sqlite_master WHERE type="table" and name="Good_Raw_Data')
            if c.fetchone()[0]==1:
                conn.close()
                f=open('Training_logs/dbConnectionLog.txt','a+')
                self.logger.log(f,'Table created successfully in database %s'%db_name)
                f.close()

                file = open("Training_Logs/dbConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed %s database successfully" % db_name)
                file.close()

            else:
                for key in column_names.keys():
                    type=column_names[key]
                    try:
                        conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                    except:
                        conn.execute('CREATE TABLE Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))
                conn.close()

                file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()

                file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed %s database successfully" % db_name)
                file.close()

        except Exception as e:
            file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % db_name)
            file.close()
            raise e

    def insertGoodRawDataToTable(self,database):
        conn=self.dbConnection(database)
        goodfilePath=self.goodFilePath
        badfilePath=self.badFilePath
        files_in_gooddata=[f for f in listdir(goodfilePath)]
        log_file=open('Training_logs/InsertDBlog.txt','a+')
        try:
            for file in files_in_gooddata:
                with open('TrainingSet_Rawfiles_after_validation/GoodData','a+') as f:
                    csv=csv.reader(f,delimiter='\n')
                    for line in enumerate(csv):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT INTO Good_Raw_Data VALUES ({values})'.format(values=(list_)))
                                self.logger.log()





