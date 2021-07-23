import pandas as pd
from app_logger.App_Logger import App_logger
from os import listdir

class dataTransformPredict:
    def __init__(self):
        self.logger=App_logger()
        self.goodData_path='Prediction_Rawfiles_after_validation/GoodData/'

    def addQuotestoSringValuesInColumn(self):
        log_file = open('Prediction_logs/quotesToStringValues.txt','a+')
        try:
            files=[f for f in listdir(self.goodData_path)]
            for file in files:
                data=pd.read_csv(self.goodData_path+'/'+file)
                data['class']=data['class'].apply(lambda x:"'"+str(x)+"'")
                data.to_csv(self.goodData_path+ "/" + file, index=None, header=True)
                self.logger.log(log_file," %s: Quotes added successfully!!" % file)
        except Exception as e:
            self.logger.log(log_file,"Error in adding quotes to string values::%s"%e)
            log_file.close()
        log_file.close()