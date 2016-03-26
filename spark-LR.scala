
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.optimization._

class spark_csv {

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val descPath="D:/data/adult.desc"
  val csvPath="D:/data/adult_data.csv"
  val test_csvPath="D:/data/adult_test.csv"

  def loadDesc(descPath:String):(StructType,Seq[Int])={
      val desc = sc.textFile(descPath).filter(line => line.trim().length>0).collect()
      val schema = StructType(desc.map(_.split(":")).map(namewithType => StructField(namewithType(0).trim(),
              namewithType(1).trim match {
                  // case "continuous." => DoubleType
                  case _ => StringType
              }
          )))

      val enumList = (0 to desc.size-1).filter(!desc(_).split(":")(1).trim().toString.startsWith("continuous"))

      (schema,enumList)
  }

  def loadCSV(csvPath:String , descPath:String)={
       val csv = sc.textFile(csvPath).filter(line=> line.trim().length>0)
       val (schema,enumList) = loadDesc(descPath)
       val rowRDD =  csv.map(line => {
            if(line(line.length()-1)=='.') 
              line.substring(0, line.length() -1)
            else
              line
          }).map(_.split(",").map(_.trim()))map(t => Row(t:_*))
       val dataFrame = sqlContext.createDataFrame(rowRDD, schema)
       dataFrame.cache
       (dataFrame,enumList)
  }

  val trainData = loadCSV(csvPath, descPath)
  val testData = loadCSV(test_csvPath, descPath)
  
  
  def getCols(dataFrame:DataFrame, enumList:Seq[Int])={

    val columns = dataFrame.columns

    val enumSets = enumList.par.map(enumIndex => dataFrame.select(columns(enumIndex)).distinct.collect.sortWith(_.getString(0)<_.getString(0)))

    val newCols = columns.take(columns.size-1).zipWithIndex.map(t => {
        val findIndex = enumList.indexOf(t._2)
        if(findIndex == -1){
            Array(t._1)
        } else {              
            //enumSets(findIndex).zipWithIndex.map(t._1 + ":" + _(0))
            val dummy = enumSets(findIndex)(0).getString(0)
            
            enumSets(findIndex).zipWithIndex.map(name => {
              if (name._2>0) {
                t._1 + "--" + dummy + ":" + name._1(0)
              } else {
                ""
              }
            }).drop(1)
        }
    })
    
    (enumSets, newCols)
  }

  val allData = trainData._1.unionAll(testData._1)
  val enumList = trainData._2
  val enumCols = getCols(allData, enumList)
  
  def convertFactors(dataFrame:DataFrame, enumList:Seq[Int], enumSets:scala.collection.parallel.ParSeq[Array[Row]])={
      
      val feature = dataFrame.map{ row => (0 to row.size-1).par.map(colIndex => {
          val colValue = row.getString(colIndex)
          val enumCol=(0 to enumList.size-1).filter(enumList(_)==colIndex).map(t => {
                  val enums = enumSets(t).zipWithIndex
                  if (colValue!=enums(0)._1.getString(0)){
                      val index = enums.filter(_._1.getString(0)==colValue)(0)._2
                      Vectors.sparse(enums.size-1,Array(index-1),Array(1))
                  } else {
                      Vectors.sparse(enums.size-1,Array(0),Array(0))
                  }
              })

          if (enumCol.size==0){
              Vectors.sparse(1,Array(0),Array(colValue.toDouble))
          } else {
              enumCol(0)
          }})
      }

      feature
  }

  val trainFt = convertFactors(trainData._1, enumList, enumCols._1)
  val testFt = convertFactors(testData._1, enumList, enumCols._1)
  
  def crossFullCols(dataFt:org.apache.spark.rdd.RDD[scala.collection.parallel.immutable.ParSeq[org.apache.spark.mllib.linalg.Vector]]){
    dataFt.map(row => {
      val crossVal = row.zipWithIndex.filter(_._2!=row.size-1).map(t => {       
        val col = t._1.asInstanceOf[SparseVector]        
        (0 to t._2-1).map(row(_).asInstanceOf[SparseVector].values(0) * col.values(0))        
      })      
    })
  }
  
  def crossTwoRawCols(dataFt:org.apache.spark.rdd.RDD[scala.collection.parallel.immutable.ParSeq[org.apache.spark.mllib.linalg.Vector]],
    colA:Int, colB:Int, newCols:  Array[Array[String]])={
      
      val newFt = dataFt.map(row => {
        val colAval = row.apply(colA).asInstanceOf[SparseVector]     
        val colBval = row.apply(colB).asInstanceOf[SparseVector]
        val newIndex = colAval.indices(0) * colBval.size + colBval.indices(0)
        row :+ Vectors.sparse(colAval.size*colBval.size, Array(newIndex),Array(colAval.values(0)*colBval.values(0)))
      })
      
      val crossCols = newCols(colA).map(t=> newCols(colB).map(t+"___"+_)).flatten[String]
      (newFt, newCols :+ crossCols)
  }
  
  def crossFullRawCols(dataFt:org.apache.spark.rdd.RDD[scala.collection.parallel.immutable.ParSeq[org.apache.spark.mllib.linalg.Vector]],
    enumArr:Array[Array[String]]) ={
    
    var newFtCols = (dataFt, enumArr)
    (0 to newFtCols._2.size-1).foreach( index => {
      (0 to index-1).foreach( t => newFtCols = crossTwoRawCols(newFtCols._1, t, index, newFtCols._2))
    })
    
    newFtCols
  }
  
  val trainCrossFtCols = crossFullRawCols(trainFt, enumCols._2)
  val testCrossFtCols = crossFullRawCols(testFt, enumCols._2)
  
  
  def genLabeledPointwithID(feature:org.apache.spark.rdd.RDD[scala.collection.parallel.immutable.ParSeq[org.apache.spark.mllib.linalg.Vector]],
      labelIndex:Int )={
      val dataWithID = feature.zipWithIndex()

      val labeled = dataWithID.map(row => {
          val label = row._1.apply(labelIndex)(0)
          val fArray = row._1.zipWithIndex.filter(_._2!=labelIndex).map(_._1).map(_.toDense.toArray).flatten.toArray
          
          (row._2, LabeledPoint( label, Vectors.dense(fArray)))
      })

      labeled
  }

  val trainLabel = genLabeledPointwithID(trainCrossFtCols._1, enumList.last)
  val testLabel = genLabeledPointwithID(testCrossFtCols._1, enumList.last)
  
  def overSample(labeled:org.apache.spark.rdd.RDD[(Long, org.apache.spark.mllib.regression.LabeledPoint)])={      
      // labeled.cache
      val pOldLabel = labeled.filter(_._2.label==1)
      println("sample positive: " + pOldLabel.count)
      val nOldLabel = labeled.filter(_._2.label==0)
      println("sample negotive: " + nOldLabel.count)
      
      var high = pOldLabel.count
      var low = nOldLabel.count
      var mulLabel = nOldLabel
      var factor = nOldLabel
      if (high!=low) {
        if (high<low) {
          val tmp = low
          low = high
          high = tmp
          mulLabel = pOldLabel
          factor = pOldLabel
        }
        
        val mul = (high/low).toInt
        val residue = (high-low*mul).toInt
        (1 to mul-2).foreach(t=> mulLabel=mulLabel.union(factor))
        
        mulLabel = labeled.union(mulLabel.union(sc.makeRDD(factor.takeSample(false,residue,13L))))
      }
      // labeled.unpersist()
      mulLabel
  }
 
  // val overtrainLabel = overSample(trainLabel)
  // val overtestLabel = overSample(testLabel)
  
  def outPrecision(predictionAndLabels: org.apache.spark.rdd.RDD[(Double, Double)])={
    predictionAndLabels.unpersist()
    predictionAndLabels.cache
    val pLabels = predictionAndLabels.filter(_._2==1).count
    val nLabels = predictionAndLabels.filter(_._2==0).count
    val tp = predictionAndLabels.filter(t => t._1==1 && t._2==1).count
    val tn = predictionAndLabels.filter(t => t._1==0 && t._2==0).count   
    println("Precision: " + (tp)/(pLabels).toDouble)
    println("Accuracy: " + (tp+tn)/(pLabels+nLabels).toDouble)
    println("Recall: " + (tp)/(nLabels-tn+tp).toDouble)    
  }
  
  val result = outPrecision(predictionAndLabels)
  
  def selectWeightCol(model:org.apache.spark.mllib.regression.GeneralizedLinearModel, newCols:Array[Array[String]])={
    val weightCols = Array(model.weights.toArray.map(_.toString), newCols.flatten[String]).transpose
    val weightAsc = weightCols.sortWith(_(0).toDouble > _(0).toDouble)
    weightAsc
  }
  
  val weightCol = selectWeightCol(model, enumCols._2)
  
  
  def logist()={
      
      //val splits = labeled.map(_._2).randomSplit(Array(0.8, 0.2), seed = 11L)
      
      // val splits = labeled.union(mulLabel).map(_._2).randomSplit(Array(0.8, 0.2), seed = 11L)
      
      // training.unpersist()
      // val training = splits(0)      
      // training.cache()
      // val test = splits(1)
     

      val lr = new LogisticRegressionWithLBFGS().setIntercept(true)
      lr.optimizer.setUpdater(new L1Updater).setRegParam(0.0000001)
      
      // val model = lr.run(training)
      
      overtrainLabel.unpersist()
      overtrainLabel.cache
      
      /* logist already set scaler */
      
      // val scaler = new StandardScaler(withMean = true, withStd = true).fit(trainLabel.map(x => x.features))
      // val trainLabel2 = trainLabel.map(x => (x.label, scaler.transform(Vectors.dense(x.features.toArray))))
      
      val model = lr.run(trainLabel2.map(_._2))
      
      // val scaler2 = new StandardScaler(withMean = true, withStd = true).fit(testLabel.map(x => x.features))
      // val testLabel2 = testLabel.map(x => (x.label, scaler2.transform(Vectors.dense(x.features.toArray))))
      
      val predictionAndLabels = testLabel2.map(_._2).map { case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
      }
      
      
      
      //  val metrics = new MulticlassMetrics(predictionAndLabels)
      //  val precision = metrics.precision
      
      //  println("Precision = " + precision)
      //  model.save(sc, "D:/data/")
      //  val sameModel = LogisticRegressionModel.load(sc, "myModelPath")
      
      
      // Get evaluation metrics.
      val metrics = new BinaryClassificationMetrics(predictionAndLabels)
      val auROC = metrics.areaUnderROC()

      println("Loss of each step in training process")
      loss.foreach(println)
      println("Area under ROC = " + auROC)

  }
    
    
    

}



check row with newCol name:
val checkRow = Array(labeled.take(2)(1)._2.features.toArray.map(_.toString), newCols).transpose
checkRow.filter(_(0).toDouble!=0)
 

/*
===============================
Outcome record:

raw -------------
32561
1:  7841
0:  24720

-----------------
reg=0

1558
5031

Precision: 0.6007702182284981
Accuracy: 0.8539990893914099
Recall: 0.7335423197492164



---------
balance

1: 4740
0: 5031

Precision: 0.8339662447257384
Accuracy: 0.8238665438542626
Recall: 0.8088807039083282

----
reg:0.01
Precision: 0.8559071729957806
Accuracy: 0.8079009313273974
Recall: 0.7726147400495144

reg:0.0001
Precision: 0.8350210970464135
Accuracy: 0.8238665438542626
Recall: 0.8082499489483357

reg:0.000001
Precision: 0.8343881856540084
Accuracy: 0.8236618565141746
Recall: 0.8082975679542204

----
balance:
reg:0.000001
Precision: 0.843530591775326
Accuracy: 0.825479233226837
Recall: 0.8128745408853664

-----
use test
32561
1  7841
0  24720


16281
1  3846
0  12435

reg:0.000001
Precision: 0.6006240249609984
Accuracy: 0.85283459246975
Recall: 0.7287066246056783


------
overSample:
train
49440   24720
test
24870   12435

reg:0.01
Precision: 0.8722155207076799
Accuracy: 0.8086449537595497
Recall: 0.773829908675799

reg:0.00001
Precision: 0.8398069963811822
Accuracy: 0.8194209891435464
Recall: 0.8069077422345851

reg:0.0000001
Precision: 0.8406111781262565
Accuracy: 0.8200643345396059
Recall: 0.8074308666769658


*/
