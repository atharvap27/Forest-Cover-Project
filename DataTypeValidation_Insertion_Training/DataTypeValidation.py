import shutil
import sqlite3
from os import listdir
import os
import csv
from app_logger.App_Logger import App_logger

class DataBaseOperations:
    def __init__(self):
        self.logger=App_logger()
        self.path='Training_database/'
        self.badFilePath='TrainingSet_Rawfiles_after_validation/BadData'
        self.goodFilePath='TrainingSet_Rawfiles_after_validation/GoodData'

    def dbConnection(self,db_name):
        try:
            conn=sqlite3.connect(self.path+db_name+'.db')
            f=open('Training_logs/dbConnectionLog.txt','a+')
            self.logger.log(f,'Opened %s database successfully'%db_name)
            f.close()
        except ConnectionError as c:
            f=open('Training_logs/dbConnectionLog.txt','a+')
            self.logger.log(f,'Error in connecting to database:%s'%c)
            f.close()
            raise c
        return conn

    def createTableIntoDatabase(self,db_name,column_names):
        try:
            conn=self.dbConnection(db_name)
            c=conn.cursor()
            c.execute("SELECT count(name) FROM sqlite_master WHERE type = 'table' AND name = 'Good_Raw_Data'")
            if c.fetchone()[0]==1:
                conn.close()
                f=open('Training_logs/dbTableCreateLog.txt','a+')
                self.logger.log(f,'Table created successfully in database %s'%db_name)
                f.close()

                file = open("Training_logs/dbConnectionLog.txt", 'a+')
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

                file = open("Training_logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()

                file = open("Training_logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed %s database successfully" % db_name)
                file.close()

        except Exception as e:
            file = open("Training_logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            file = open("Training_logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % db_name)
            file.close()
            raise e

    def insertGoodRawDataToTable(self,database):
        conn=self.dbConnection(database)
        goodFilePath=self.goodFilePath
        badFilePath=self.badFilePath
        onlyfiles=[f for f in listdir(goodFilePath)]
        log_file=open('Training_logs/InsertDBlog.txt','a+')
        count=1
        for file in onlyfiles:
            try:
                with open(goodFilePath+'/'+file,"r") as f:
                    next(f)
                    reader=csv.reader(f,delimiter='\n')
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values=(list_)))
                                self.logger.log(log_file,"%s: File loaded successfully!!" % file)
                                conn.commit()
                                print("row: " +str(count))
                                count+=1
                            except Exception as e:
                                raise e
            except Exception as e:
                conn.rollback()
                self.logger.log(log_file,'Error in creating table :%s'%e)
                shutil.move(goodFilePath+'/'+ file,badFilePath)
                self.logger.log(log_file,' %s file moved to BadData successfully'%file)
                log_file.close()
                conn.close()

        conn.close()
        log_file.close()

    def selectingDatafromTableinDatabasetoCSV(self,database):
        self.fileFromDb='TrainingFile_from_db/'
        self.db_file='InputFile.csv'
        log_file=open('Training_logs/csvDatafromDb.txt','a+')
        try:
            conn=self.dbConnection(database)
            sql_cmd="SELECT * FROM Good_Raw_Data"
            cursor=conn.cursor()
            cursor.execute(sql_cmd)
            result=cursor.fetchall()
            column_names=[i[0] for i in cursor.description]
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)
            csv_fromdb=csv.writer(open(self.fileFromDb + self.db_file, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')

            csv_fromdb.writerow(column_names)
            csv_fromdb.writerows(result)
            self.logger.log(log_file,'File exported successfully')
            log_file.close()

        except Exception as e:
            self.logger.log(log_file,'file exporting failed!!:%s'%e)
            log_file.close()








