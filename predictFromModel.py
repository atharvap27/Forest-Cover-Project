import pandas as pd

from app_logger import App_Logger
from file_operations import fileOperations
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction
from prediction_raw_data_validation.predictionDataValidation import prediction_data_validation

class prediction:
    def __init__(self,path):
        self.file_object = open("Prediction_logs/Prediction_Log.txt", 'a+')
        self.log_writer = App_Logger.App_logger()
        self.pred_data_val = prediction_data_validation(path)

    def predictionFromModel(self):
        try:
            self.pred_data_val.deletePredictionFile()
            self.log_writer.log(self.file_object,'Start of prediction')
            data_getter=data_loader_prediction.Data_Getter_Pred(self.file_object,self.log_writer)
            data=data_getter.get_data()
            preprocess=preprocessing.preprocessing(self.file_object,self.log_writer)
            data=preprocess.scale_data(data)
            #data=preprocess.enocdeCategoricalvalues(data)
            file_loader=fileOperations.File_Oerations(self.file_object,self.log_writer)
            kmeans=file_loader.load_model('KMeans')
            clusters=kmeans.predict(data)
            data['clusters']=clusters
            clusters=data['clusters'].unique()
            result=[]
            for i in clusters:
                cluster_data=data[data['clusters']==i]
                cluster_data=cluster_data.drop(['clusters'],axis=1)
                model_name=file_loader.find_correct_model_file(i)
                model=file_loader.load_model(model_name)
                for val in (model.predict(cluster_data)):
                    if val == 0:
                        result.append("Lodgepole_Pine")
                    elif val == 1:
                        result.append("Spruce_Fir")
                    elif val == 2:
                        result.append("Douglas_fir")
                    elif val == 3:
                        result.append("Krummholz")
                    elif val == 4:
                        result.append("Ponderosa_Pine")
                    elif val == 5:
                        result.append("Aspen")
                    elif val == 6:
                        result.append("Cottonwood_Willow")
                result=pd.DataFrame(result,columns=['Predictions'])

                result.to_csv("Prediction_Output_File/Predictions.csv", header=True,mode='a+')
                self.log_writer.log(self.file_object, 'End of Prediction')
        except Exception as e:
            self.log_writer.log(self.file_object, 'Error occured while running the prediction!! Error:: %s' % e)
            raise e
        path='Prediction_Output_File/Predictions.csv'
        return path





