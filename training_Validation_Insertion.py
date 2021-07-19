
from app_logger.App_Logger import App_logger
from trainset_raw_data_validation.rawValidation import Raw_data_validation
from Training_DataTransformation.Train_DataTransformation import DataTransformation
from DataTypeValidation_Insertion_Training.DataTypeValidation import DataBaseOperations

class train_validation():

    def __init__(self,path):
        self.file_object=open('Training_logs/Training_Main_Log.txt','a+')
        self.raw_data=Raw_data_validation(path)
        self.dataTransform=DataTransformation()
        self.log_writer=App_logger()
        self.dbOperation=DataBaseOperations()

    def train_validation(self):
        try:
            self.log_writer.log(self.file_object,'Validation on training files has started')
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_name, number_of_columns=self.raw_data.values_from_schema()
            regex=self.raw_data.regex_creation()
            self.raw_data.validateFileName(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)
            self.raw_data.columnValidation(number_of_columns)
            self.raw_data.missingValuesInColumnValidation()
            self.log_writer.log(self.file_object,'Raw Training Data Validation completed')
            self.log_writer.log(self.file_object,'Data Transformation has started')
            self.dataTransform.addQuotestoSringValuesInColumn()
            self.log_writer.log(self.file_object,'Completed the Data Transformation')
            self.log_writer.log(self.file_object,'Creating Training_database through the given column names')
            self.dbOperation.createTableIntoDatabase('Training',column_name)
            self.log_writer.log(self.file_object,'Created a table into Training_database')
            self.log_writer.log(self.file_object, 'Insertion of data into table started')
            self.dbOperation.insertGoodRawDataToTable('Training')
            self.log_writer.log(self.file_object, 'Successfuly inserted the data to the table')
            self.log_writer.log(self.file_object, 'Deleting Good data folder')
            self.raw_data.deleteExistingRawGoodDataFolder()
            self.log_writer.log(self.file_object, 'Deleted the good data folder')
            self.log_writer.log(self.file_object, 'Archiving the bad data folder and then deleting it')
            self.raw_data.ArchiveBadData()
            self.raw_data.deleteExistingRawBadDataFolder()
            self.log_writer.log(self.file_object,'Archived the bad data folder and deleted it')
            self.log_writer.log(self.file_object, "Validation Operation completed!!")
            self.log_writer.log(self.file_object, "Extracting csv file from table")
            self.dbOperation.selectingDatafromTableinDatabasetoCSV('Training')
            self.log_writer.log(self.file_object, "CSV file extraction completed!!")
            self.file_object.close()

        except Exception as e:
            self.file_object.close()
            raise Exception


