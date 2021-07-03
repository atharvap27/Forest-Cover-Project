import sqlite3
import os
from os import listdir
from datetime import datetime
from app_logger.App_Logger import App_logger
import json
import re
import shutil
import pandas as pd

class Raw_data_validation:
    def __init__(self,path):
        self.batch_directory = path
        self.schema_path = 'training_set_schema.json'
        self.logger = App_logger()

    def values_from_schema(self):
        try:
            with open(self.schema_path,'r') as f:
                info=json.load(f)
                f.close()
            Sample_file_name=info['SampleFileName']
            LengthOfDateStampInFile=info['LengthOfDateStampInFile']
            LengthOfTimeStampInFile=info['LengthOfTimeStampInFile']
            column_name = info['ColName']
            number_of_columns = info['NumberofColumns']
            file=open('Training_logs/valuesfromSchemaValidationLog.txt','a+')
            message = "LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % number_of_columns + "\n"
            self.logger.log(file,message)
            file.close()

        except KeyError:
            file = open('Training_logs/valuesfromSchemaValidationLog.txt', 'a+')
            self.logger.log(file,'KeyError:incorrect key passed')
            file.close()
            raise KeyError

        except ValueError:
            file = open('Training_logs/valuesfromSchemaValidationLog.txt', 'a+')
            self.logger.log(file,'ValueError:value not available in training_set_schema')
            file.close()
            raise ValueError

        except Exception as e:
            file = open('Training_logs/valuesfromSchemaValidationLog.txt', 'a+')
            self.logger.log(file,'Exception error:'+str(e))
            file.close()
            raise Exception()
        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_name, number_of_columns

    def createDirectoryforGoodBadRawData(self):
        try:
            path=os.path.join('TrainingSet_Rawfiles_after_validation/'+'GoodData')
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join('TrainingSet_Rawfiles_after_validation/' + 'BadData')
            if not os.path.isdir(path):
                os.makedirs(path)
        except OSError as er:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,'%s directory not created due to error'%er)
            self.logger.log(file,'Exiting the createDirectoryforGoodBadRawData method of class RawDataValidation')
            file.close()
            raise OSError

    def regex_creation(self):
        regex="['forest_cover']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def deleteExistingRawGoodDataFolder(self):
        try:
            path='TrainingSet_Rawfiles_after_validation/'
            if os.path.isdir(path+'GoodData'):
                shutil.rmtree(path+'GoodData')
                file=open('Training_Logs/GeneralLog.txt','a+')
                self.logger.log(file,'Successfully deleted the GoodData file of TrainingSet_Rawfiles_after_validation folder!!')
                file.close()
        except OSError as oe:
            file=open('Training_Logs/GeneralLog.txt','a+')
            self.logger.log(file,'OS error:'%oe)
            self.logger.log(file,'Error in deleting GoodData file from TrainingSet_Rawfiles_after_validation folder')
            file.close()
            raise OSError()

    def deleteExistingRawBadDataFolder(self):
        try:
            path='TrainingSet_Rawfiles_after_validation/'
            if os.path.isdir(path+'BadData'):
                shutil.rmtree(path+'BadData')
                file=open('Training_Logs/GeneralLog.txt','a+')
                self.logger.log(file,'Successfully deleted the BadData file of TrainingSet_Rawfiles_after_validation folder!!')
                file.close()
        except OSError as s:
            file=open('Training_Logs/GeneralLog.txt','a+')
            self.logger.log(file,'OS error:'%s)
            self.logger.log(file,'Error in deleting BadData file from TrainingSet_Rawfiles_after_validation folder')
            file.close()
            raise OSError()

    def ArchiveBadData(self):
        now=datetime.now()
        date=now.date()
        time=now.strftime('%H%M%S')

        try:
            src='TrainingSet_Rawfiles_after_validation/BadData/'
            if os.path.isdir(src):
                path='ArchivedBadData'
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest='ArchivedBadData/BadData'+str(date)+'_'+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files=os.listdir(src)
                for file in files:
                    if file not in os.listdir(dest):
                        shutil.move(src + file,dest)
                fil=open('Training_Logs/GeneralLog.txt','a+')
                self.logger.log(fil,'Successfully moved file to ArchivedBadData folder')
                fil.close()
                path='TrainingSet_Rawfiles_after_validation/'
                if os.path.isdir(path+'BadData/'):
                    shutil.rmtree(path+'BadData/')
                fil=open('Training_Logs/GeneralLog.txt','a+')
                self.logger.log(fil, 'Successfully deleted BadRaw data files')
                fil.close()

        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,'Error occured in moving BadData files to archiveBadData folder:%s'%e)
            file.close()
            raise e

    def validateFileName(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        self.deleteExistingRawGoodDataFolder()
        self.deleteExistingRawBadDataFolder()
        self.createDirectoryforGoodBadRawData()
        onlyfiles=[f for f in self.batch_directory]
        try:
            f=open("Training_Logs/GeneralLog.txt", 'a+')
            for filename in onlyfiles:
                if (re.match(regex,filename)):
                    split=re.split('.csv',filename)
                    split=re.split('_',split[0])
                    if len(split[2])==LengthOfDateStampInFile:
                        if len(split[3])==LengthOfTimeStampInFile:
                            shutil.move("Training_Batch_Files/" + filename, "TrainingSet_Rawfiles_after_validation/GoodData")
                            self.logger.log(f, "Valid File name!! File moved to GoodData Folder :: %s" % filename)
                        else:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/BadData")
                            self.logger.log(f, "Invalid File Name!! File moved to BadData Folder :: %s" % filename)

                    else:
                        shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/BadData")
                        self.logger.log(f, "Invalid File Name!! File moved to BadData Folder :: %s" % filename)

                else:
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/BadData")
                    self.logger.log(f, "Invalid File Name!! File moved to BadData Folder :: %s" % filename)

            f.close()

        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f,'Error in validating the raw file name.Error::%s'%e)
            f.close()
            raise e

    def columnValidation(self,number_of_columns):
        try:
            f=open('Training_logs/columnValidationLog.txt', 'a+')
            self.logger.log(f,'Column length validation has started')
            for file in listdir('TrainingSet_Rawfiles_after_validation/GoodData/'):
                csv=pd.read_csv('TrainingSet_Rawfiles_after_validation/GoodData/'+file)
                if csv.shape[1]==number_of_columns:
                    pass
                else:
                    shutil.move("TrainingSet_Rawfiles_after_validation/GoodData/" + file, "TrainingSet_Rawfiles_after_validation/BadData")
                    self.logger.log(f,'Invalid column length..files moved to BadData folder')
            self.logger.log(f,'Column Length validation successful!!')
        except OSError as s:
            f=open('Training_logs/columnValidationLog.txt','a+')
            self.logger.log(f,'Error in column Validation: %s'%s)
            f.close()
            raise s

        except Exception as e:
            f=open('Training_logs/columnValidationLog.txt','a+')
            self.logger.log(f,'Exception error occured ::%s'%e)
            f.close()
            raise e


    def missingValuesInColumnValidation(self):
        try:
            f=open('Training_logs/missingValuesInColumn.txt','a+')
            self.logger.log(f,'missing value validation started!')
            for file in listdir('TrainingSet_Rawfiles_after_validation/GoodData/'):
                csv_file=pd.read_csv('TrainingSet_Rawfiles_after_validation/GoodData/'+file)
                count=0
                for column in csv_file:
                    if (len(csv_file[column])-csv_file[column].count())==len(csv_file[column]):
                        count+=1
                        shutil.move('TrainingSet_Rawfiles_after_validation/GoodData'+file,'TrainingSet_Rawfiles_after_validation/BadData')
                        self.logger.log(f,'Invalid column length. File moved to BadData folder::%s'%file)
                        break
                    if count==0:
                        csv_file.to_csv('TrainingSet_Rawfiles_after_validation/GoodData'+file)
        except OSError as os:
            f=open('Training_logs/missingValuesInColumn.txt','a+')
            self.logger.log(f,'Error in moving file to BadData folder::%s'%os)
            f.close()
            raise os

        except Exception as e:
            f=open('Training_logs/missingValuesInColumn.txt','a+')
            self.logger.log(f,'Error occured::%s'%e)
            f.close()
            raise e























