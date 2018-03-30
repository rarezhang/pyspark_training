"""
collaborative filtering using ALS algorithm 

build the recommendation model and evaluate it on training data

"""


from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

if __name__ == "__main__":

   sc = SparkContext(appName="Pspark mllib Example") # run any Spark application - a driver program starts
   
   
   data = sc.textFile("test.data")
   
   ratings = data.map(lambda l: l.split(','))\
      .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))
      # class Rating(user: Int, product: Int, rating: Double) extends Product with Serializable
      
   
   # Build the recommendation model using Alternating Least Squares
   rank = 10
   numIterations = 10
   model = ALS.train(ratings, rank, numIterations)
   
   # Evaluate the model on training data
   testdata = ratings.map(lambda p: (p[0], p[1]))
   predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
   ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
   MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
   print("Mean Squared Error = " + str(MSE))
   
'''   
   # Save and load model
   model.save(sc, "target/tmp/myCollaborativeFilter")
   sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")
'''   