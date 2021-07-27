from app_logger.App_Logger import App_logger
from prediction_raw_data_validation.predictionDataValidation import prediction_data_validation
from Predicting_DataTransformation.Prediction_DataTransformation import dataTransformPredict
from DataTypeValidation_Insertion_Prediction.Prediction_datatype_validation import DataBaseOperations

class pred_validation:

    def __init__(self,path):
        self.file_object=open('Prediction_logs/Prediction_Log.txt','a+')
        self.raw_data=prediction_data_validation(path)
        self.dataTransform=dataTransformPredict()
        self.log_writer=App_logger()
        self.dbOperation=DataBaseOperations()

    def prediction_validation(self):
        try:

            self.log_writer.log(self.file_object, 'Start of Validation on files for prediction!!')
            # extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.values_from_schema()
            # getting the regex defined to validate filename
            regex = self.raw_data.regex_creation()
            # validating filename of prediction files
            self.raw_data.validateFileName(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            # validating column length in the file
            self.raw_data.columnValidation(noofcolumns)
            # validating if any column has all values missing
            self.raw_data.missingValuesInColumnValidation()
            self.log_writer.log(self.file_object, "Raw Data Validation Complete!!")

            self.log_writer.log(self.file_object,
                                "Creating Prediction_Database and tables on the basis of given schema!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dbOperation.createTableIntoDatabase('Prediction', column_names)
            self.log_writer.log(self.file_object, "Table creation Completed!!")
            self.log_writer.log(self.file_object, "Insertion of Data into Table started!!!!")
            # insert csv files in the table
            self.dbOperation.insertGoodRawDataToTable('Prediction')
            self.log_writer.log(self.file_object, "Insertion in Table completed!!!")
            self.log_writer.log(self.file_object, "Deleting Good Data Folder!!!")
            # Delete the good data folder after loading files in table
            self.raw_data.deleteExistingRawGoodDataFolder()
            self.log_writer.log(self.file_object, "Good_Data folder deleted!!!")
            self.log_writer.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            self.raw_data.ArchiveBadData()
            self.log_writer.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.file_object, "Validation Operation completed!!")
            self.log_writer.log(self.file_object, "Extracting csv file from table")
            # export data in table to csvfile
            self.dbOperation.selectingDatafromTableinDatabasetoCSV('Prediction')

        except Exception as e:
            raise e