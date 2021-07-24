import sqlite3
import os
from os import listdir
from datetime import datetime
from app_logger.App_Logger import App_logger
import json
import re
import shutil
import pandas as pd

class prediction_data_validation:
    def __init__(self,path):
        self.batch_directory = path
        self.schema_path = 'schema_prediction.json'
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
            file = open('Prediction_logs/valuesfromSchemaValidationLog.txt', 'a+')
            self.logger.log(file,'KeyError:incorrect key passed')
            file.close()
            raise KeyError

        except ValueError:
            file = open('Prediction_logs/valuesfromSchemaValidationLog.txt', 'a+')
            self.logger.log(file,'ValueError:value not available in training_set_schema')
            file.close()
            raise ValueError

        except Exception as e:
            file = open('Prediction_logs/valuesfromSchemaValidationLog.txt', 'a+')
            self.logger.log(file,'Exception error:'+str(e))
            file.close()
            raise Exception()
        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_name, number_of_columns

    def createDirectoryforGoodBadRawData(self):
        try:
            path=os.path.join('Prediction_Rawfiles_after_validation/'+'GoodData')
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join('Prediction_Rawfiles_after_validation/' + 'BadData')
            if not os.path.isdir(path):
                os.makedirs(path)
        except OSError as er:
            file = open("Prediction_logs/GeneralLog.txt", 'a+')
            self.logger.log(file,'%s directory not created due to error'%er)
            self.logger.log(file,'Exiting the createDirectoryforGoodBadRawData method of class prediction_data_validation')
            file.close()
            raise OSError

    def regex_creation(self):
        regex="['forest_cover']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def deleteExistingRawGoodDataFolder(self):
        try:
            path='Prediction_Rawfiles_after_validation/'
            if os.path.isdir(path+'GoodData/'):
                shutil.rmtree(path+'GoodData/')
                file=open('Prediction_logs/GeneralLog.txt','a+')
                self.logger.log(file,'Successfully deleted the GoodData file of Prediction_Rawfiles_after_validation folder!!')
                file.close()
        except OSError as oe:
            file=open('Prediction_logs/GeneralLog.txt','a+')
            self.logger.log(file,'OS error:'%oe)
            self.logger.log(file,'Error in deleting GoodData file from Prediction_Rawfiles_after_validation folder')
            file.close()
            raise OSError()

    def deleteExistingRawBadDataFolder(self):
        try:
            path='Prediction_Rawfiles_after_validation/'
            if os.path.isdir(path+'BadData/'):
                shutil.rmtree(path+'BadData/')
                file=open('Prediction_logs/GeneralLog.txt','a+')
                self.logger.log(file,'Successfully deleted the BadData file of Prediction_Rawfiles_after_validation folder!!')
                file.close()
        except OSError as s:
            file=open('Prediction_logs/GeneralLog.txt','a+')
            self.logger.log(file,'OS error:'%s)
            self.logger.log(file,'Error in deleting BadData file from Prediction_Rawfiles_after_validation folder')
            file.close()
            raise OSError()

    def ArchiveBadData(self):
        now=datetime.now()
        date=now.date()
        time=now.strftime('%H%M%S')

        try:
            path= "PredictionArchivedBadData"
            if not os.path.isdir(path):
                os.makedirs(path)
            source = 'Prediction_Rawfiles_after_validation/BadData/'
            dest = 'PredictionArchivedBadData/BadData_' + str(date)+"_"+str(time)
            if not os.path.isdir(dest):
                os.makedirs(dest)
            files = os.listdir(source)
            for f in files:
                if f not in os.listdir(dest):
                    shutil.move(source + f, dest)
            file = open("Prediction_logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Bad files moved to archive")
            path = 'Prediction_Rawfiles_after_validation/'
            if os.path.isdir(path + 'BadData/'):
                shutil.rmtree(path + 'BadData/')
            self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
            file.close()
        except OSError as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise OSError

        except Exception as e:
            file = open("Prediction_ogs/GeneralLog.txt", 'a+')
            self.logger.log(file,'Error occurred in moving BadData files to archiveBadData folder:%s'%e)
            file.close()
            raise e

    def validateFileName(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        self.deleteExistingRawGoodDataFolder()
        self.deleteExistingRawBadDataFolder()
        self.createDirectoryforGoodBadRawData()
        onlyfiles=[f for f in listdir(self.batch_directory)]
        try:
            f=open("Prediction_logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if (re.match(regex,filename)):
                    split=re.split('.csv',filename)
                    split=(re.split('_',split[0]))
                    if len(split[2])==LengthOfDateStampInFile:
                        if len(split[3])==LengthOfTimeStampInFile:
                            shutil.move("Prediction_Batch_Files/" + filename, "Prediction_Rawfiles_after_validation/GoodData")
                            self.logger.log(f, "Valid File name!! File moved to GoodData Folder :: %s" % filename)
                        else:
                            shutil.copy("Prediction_Batch_Files/" + filename, "Prediction_Rawfiles_after_validation/BadData")
                            self.logger.log(f, "Invalid File Name!! File moved to BadData Folder :: %s" % filename)

                    else:
                        shutil.copy("Prediction_Batch_Files/" + filename, "Prediction_Rawfiles_after_validation/BadData")
                        self.logger.log(f, "Invalid File Name!! File moved to BadData Folder :: %s" % filename)

                else:
                    shutil.copy("Prediction_Batch_Files/" + filename, "Prediction_Rawfiles_after_validation/BadData")
                    self.logger.log(f, "Invalid File Name!! File moved to BadData Folder :: %s" % filename)

            f.close()

        except Exception as e:
            f = open("Prediction_logs/nameValidationLog.txt", 'a+')
            self.logger.log(f,'Error in validating the raw file name.Error::%s'%e)
            f.close()
            raise e

    def columnValidation(self,number_of_columns):
        try:
            f=open('Prediction_logs/columnValidationLog.txt', 'a+')
            self.logger.log(f,'Column length validation has started')
            for file in listdir('Prediction_Rawfiles_after_validation/GoodData/'):
                csv=pd.read_csv('Prediction_Rawfiles_after_validation/GoodData/'+file)
                if csv.shape[1]==number_of_columns:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv("Prediction_Rawfiles_after_validation/Good_Raw/" + file, index=None, header=True)
                else:
                    shutil.move("Prediction_Rawfiles_after_validation/GoodData/" + file, "Prediction_Rawfiles_after_validation/BadData")
                    self.logger.log(f,'Invalid column length..files moved to BadData folder')
            self.logger.log(f,'Column Length validation successful!!')
        except OSError as s:
            f=open('Prediction_logs/columnValidationLog.txt','a+')
            self.logger.log(f,'Error in column Validation: %s'%s)
            f.close()
            raise s

        except Exception as e:
            f=open('Prediction_logs/columnValidationLog.txt','a+')
            self.logger.log(f,'Exception error occured ::%s'%e)
            f.close()
            raise e

    def deletePredictionFile(self):

        if os.path.exists('Prediction_Output_File/Predictions.csv'):
            os.remove('Prediction_Output_File/Predictions.csv')

    def missingValuesInColumnValidation(self):
        try:
            f=open('Prediction_logs/missingValuesInColumn.txt','a+')
            self.logger.log(f,'missing value validation started!')
            for file in listdir('Prediction_Rawfiles_after_validation/GoodData/'):
                csv_file=pd.read_csv('Prediction_Rawfiles_after_validation/GoodData/'+file)
                count=0
                for column in csv_file:
                    if (len(csv_file[column])-csv_file[column].count())==len(csv_file[column]):
                        count+=1
                        shutil.move('Prediction_Rawfiles_after_validation/GoodData'+file,'Prediction_Rawfiles_after_validation/BadData')
                        self.logger.log(f,'Invalid column length. File moved to BadData folder::%s'%file)
                        break
                    if count==0:
                        csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                        csv_file.to_csv('Prediction_Rawfiles_after_validation/GoodData'+file)
        except OSError as os:
            f=open('Prediction_logs/missingValuesInColumn.txt','a+')
            self.logger.log(f,'Error in moving file to BadData folder::%s'%os)
            f.close()
            raise os

        except Exception as e:
            f=open('Prediction_logs/missingValuesInColumn.txt','a+')
            self.logger.log(f,'Error occured::%s'%e)
            f.close()
            raise e
        f.close()