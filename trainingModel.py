from app_logger import App_Logger
from best_model_finder import tuner
from data_ingestion import data_loader
from data_preprocessing import clustering,preprocessing
from file_operations import fileOperations
from sklearn.model_selection import train_test_split

class trainModel:

    def __init__(self):
        self.log_writer=App_Logger.App_logger()
        self.file_object=open('Training_logs/ModelTrainingLog.txt','a+')

    def trainingModel(self):
        self.log_writer.log(self.file_object,'Model training has started')
        try:
            data_getter=data_loader.Data_loader(self.file_object,self.log_writer)
            data=data_getter.get_data()
            preprocess=preprocessing.preprocessing(self.file_object,self.log_writer)
            data=preprocess.enocdeCategoricalvalues(data)
            X=data.drop(['class'],axis=1)
            Y=data['class']

            X,Y=preprocess.handleImbalancedDataset(X,Y)

            kmeans=clustering.KmeansClustering(self.file_object,self.log_writer)
            num_of_clusters=kmeans.elbow_method(X)
            X=kmeans.create_cluster(X,num_of_clusters)
            X['Labels']=Y
            list_of_clusters=X['Cluster'].unique()

            for i in list_of_clusters:
                cluster_data=X[X['Cluster']==i]
                cluster_features=cluster_data.drop(['Labels','Cluster'],axis=1)
                cluster_label=cluster_data['Labels']

                x_train,x_test,y_train,y_test=train_test_split(cluster_features,cluster_label,test_size=1/3,random_state=355)
                x_train=preprocess.scale_data(x_train)
                x_test=preprocess.scale_data(x_test)

                best_model=tuner.model_finder(self.file_object,self.log_writer)
                best_model_name,best_model=best_model.get_best_model(x_train,y_train,x_test,y_test)
                file_op=fileOperations.File_Oerations(self.file_object,self.log_writer)
                save_model=file_op.save_model(best_model,best_model_name+str(i))
            self.log_writer.log(self.file_object,'Successful end of Training')
            self.file_object.close()

        except Exception as e:
            self.log_writer.log(self.file_object,'Error has occured while training the model. Error: %s'%e)
            self.file_object.close()
            raise Exception





