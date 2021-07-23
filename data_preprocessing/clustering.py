import matplotlib.pyplot as plt
from kneed import KneeLocator
from sklearn.cluster import KMeans
from file_operations import fileOperations

class KmeansClustering:

    def __init__(self,file_object,logger_object):
        self.file_object=file_object
        self.logger_object=logger_object

    def elbow_method(self,data):
        self.logger_object.log(self.file_object,'Entered the elbow_method of class KmeansCkustering')
        wcss=[]
        try:
            for i in range(1,11):
                kmeans=KMeans(n_clusters=i,init='k-means++',random_state=42)
                kmeans.fit(data)
                wcss.append(kmeans.inertia_)
            plt.plot(range(1, 11), wcss)
            plt.xlabel('Number of clusters')
            plt.ylabel('wcss')
            plt.title('Elbow method')
            plt.savefig('preprocessing_data/K-means_Elbow.PNG')
            self.kn = KneeLocator(range(1, 11), wcss, curve='convex', direction='decreasing')
            self.logger_object.log(self.file_object, 'The good number of clusters are' + str(self.kn.knee) + ' . Exited the elbow method of class clustering')
            return self.kn.knee
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception has occured: %s'%e)
            self.logger_object.log(self.file_object,'Unsuccessful in plotting the elbow plot and getting the optimum number of clusters. Exiting the elbow method of class clustering')
            raise Exception

    def create_cluster(self,data,num_of_clusters):
        self.logger_object.log(self.file_object,'Entered the create cluster method of class clustering')
        self.data=data

        try:
            self.kmeans=KMeans(n_clusters=num_of_clusters,init='k-means++',random_state=42)
            self.y_kmeans=self.kmeans.fit_predict(data)
            self.file_op=fileOperations.File_Oerations(self.file_object,self.logger_object)
            self.save_model=self.file_op.save_model(self.kmeans,'Kmeans')
            self.data['Cluster']=self.y_kmeans
            self.logger_object.log(self.file_object,'Successfully created cluster.Exiting the create_cluster method of class clustering')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception has occured: %s' % e)
            self.logger_object.log(self.file_object,'Unsuccessful in creating the clusters. Exiting the create_clusters method of class clustering')
            raise Exception



