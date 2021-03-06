package com.ipm.nslab.bndf.spark.analysis

import com.ipm.nslab.bndf.commons.io.SparkReader
import com.ipm.nslab.bndf.extendedTypes.{BICValues, Median}
import com.ipm.nslab.bndf.spark.commons.Transformers
import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{abs, col, collect_list, explode, lag, lit,
  monotonically_increasing_id, sequence, sort_array, struct, when, size, broadcast}
import org.apache.spark.sql.expressions.Window

/** The Implementation of the Spike Sorting Module
 * Spike sorting procedure is applied with SparkSql and SparkML pipelines
 */
class Sorting {

  val transformers = new Transformers
  val goodnessOfFit = new GoodnessOfFit
  val sparkReader = new SparkReader

  val logger: Logger = Logger(s"${this.getClass.getName}")

  /** Applies the thresholding procedure on the signal provided by:
   * Rey, H. G., Pedreira, C. & Quian Quiroga, R. Past, present and future of spike sorting
   * techniques. Brain Res. Bull. 119, 106–117 (2015)
   * @param spark Active spark session
   * @param ChannelDataset The Channel Dataset
   * @param fileInfo The specific spike trains name (session, experiment, or any identifier that specifies the spike train set)
   * @return The Thresholded Dataset
   */
  def getThresholdDataset(spark: SparkSession, ChannelDataset: Dataset[Row], fileInfo: String): Dataset[Row] ={
    import spark.implicits._

    val med = ChannelDataset.filter($"fileInfo" === fileInfo)
                     .select(abs($"signal").alias("signal"))
                     .stat.approxQuantile("signal", Array(0.5), 0.0001).apply(0)

    val medianData = Median(fileInfo, med)

    val thresholdDataset = ChannelDataset.filter($"fileInfo" === fileInfo)
      .withColumn("threshold", lit(3 * medianData.median)/0.6745)

    thresholdDataset
  }

  /** Applies the windowing step procedure on the thresholded data set
   * @param spark Active spark session
   * @param channelDataset The Channel Dataset
   * @param fileInfo The specific spike trains name (session, experiment, or any identifier that specifies the spike train set)
   * @param n1 The lower bound of the window interval
   * @param n2 The upper bound of the window interval
   * @return The Windowed Dataset
   */
  def getWindowedSpikeDataset(spark: SparkSession, channelDataset: Dataset[Row],
                              fileInfo: String, n1: Int, n2: Int): Dataset[Row] ={
    import spark.implicits._

    val overFileInfo = Window.partitionBy("FileInfo").orderBy("Time")
    // Threshold options:
    // 1- signal < -t --> This is applied here.
    // 2- signal > t
    // 3- (signal < -t) || (signal > t)
    val thresholdedDataset = getThresholdDataset(spark, channelDataset, fileInfo)
      .withColumn("Spike", when($"signal" < -$"threshold", 1).otherwise(0))
      .select($"FileInfo", $"Time", $"Signal", $"Spike")
    

    val windowedSpikeDataset = thresholdedDataset.filter($"Spike" === 1)
      .select($"FileInfo", $"Signal", $"Spike", $"Time", lag($"Time", 1) over overFileInfo as "LAG")
      .withColumn("LAG", when($"LAG".isNull, 0).otherwise($"LAG"))
      .withColumn("diff", $"Time" - $"LAG")
      .filter($"diff" =!= 1)
      .withColumn("lwb", $"Time" - n1)
      .withColumn("uwb", $"Time" + n2)
      .withColumn("SpikeWindow", sequence($"lwb", $"uwb"))
      .withColumn("SparkSetNumber", monotonically_increasing_id)
      .select("FileInfo", "SpikeWindow", "SparkSetNumber")
      .withColumn("Time", explode($"SpikeWindow"))
      .drop("SpikeWindow")

    windowedSpikeDataset

  }

  /** Transforms sparkSql array column to SparkMl Vector Column
   * @param spark Active spark session
   * @param channelDataset The Channel Dataset
   * @param fileInfo The specific spike trains name (session, experiment, or any identifier that specifies the spike train set)
   * @param n1 The lower bound of the window interval
   * @param n2 The upper bound of the window interval
   * @return The SparkMl Transformed dataset
   */
  def getMlTransformedColumnDataset(spark: SparkSession, channelDataset: Dataset[Row],
                                    fileInfo: String, n1: Int, n2: Int): Dataset[Row] ={
    import spark.implicits._

    val windowedSpikeDataset = broadcast(getWindowedSpikeDataset(spark, channelDataset, fileInfo, n1, n2))
    val mlTransformedDataset = windowedSpikeDataset
      .join(channelDataset, Seq("FileInfo", "Time"), "left")
      .groupBy("FileInfo", "SparkSetNumber")
      .agg(sort_array(collect_list(struct("Time", "Signal", "EventCode").alias("SpikeInfo"))).alias("SpikeInfo"))
      .filter(size($"SpikeInfo") === n1 + n2 + 1)
      .withColumn("SpikeSignals", transformers.arrayToVectorUDF($"SpikeInfo.Signal"))

    mlTransformedDataset

  }

  /** Provides the spike sorting pipelines
   * 1- Applies the dimension reduction procedure by PCA
   * 2- Applies The GMM clustering for the range k (2 to 6)
   * 3- Select the best clustering number based on BIC
   * @param spark Active spark session
   * @param channelDataset The Channel Dataset
   * @param fileInfo The specific spike trains name (session, experiment, or any identifier that specifies the spike train set)
   * @return The Sorted Dataset
   */
  def simpleSorting(spark: SparkSession, channelDataset: Dataset[Row], fileInfo: String) :Dataset[Row] ={

    val mlTransformedDataset = getMlTransformedColumnDataset(spark, channelDataset, fileInfo, 30, 50).persist()

    val K = 6
    val pca = new PCA()
      .setInputCol("SpikeSignals")
      .setOutputCol("pcaFeatureColumn")
      .setK(K)
      .asInstanceOf[PipelineStage]

    val minK = 2
    val maxK = K - 1

    val gmmArray = (minK to maxK).map(k => {

      new GaussianMixture()
        .setFeaturesCol("pcaFeatureColumn")
        .setPredictionCol(s"prediction$k")
        .setProbabilityCol(s"probCol$k")
        .setK(k)
    }).toArray[PipelineStage]

    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(pca) ++ gmmArray)

    val model = pipeline.fit(mlTransformedDataset)

    // Get best model based on BIC

    val bicValues = (minK to maxK).map(k => BICValues(k, goodnessOfFit.BIC(model, k - 1 ))).toArray
    val optimalK = bicValues.sortBy(_.bic).apply(0).k

    logger.info(s"Total number of $optimalK neurons detected by GMM sorter based on BIC of value " +
      s"${bicValues.filter(_.k.equals(optimalK)).apply(0).bic}.")

    val sortedDataset = model.transform(mlTransformedDataset)
      .withColumn("vars", explode(col("SpikeInfo")))
      .select(col("FileInfo"), col("vars.Time"),
        col("vars.Signal"), col("vars.EventCode"),
        col(s"prediction$optimalK").alias("Neuron"))

    mlTransformedDataset.unpersist()
    sortedDataset
  }

  /** The Wrapper that calls sorter based on experiment name or generally available spike trains specified by
   * the meta-data information to perform sorting
   * @param spark Active spark session
   * @param metaDataset The corresponding meta-data Dataset
   * @param experimentName The required experiment name for sorting
   * @return The Sorted Dataset
   */
  def sorter(spark: SparkSession, metaDataset: Dataset[Row], experimentName: String): Dataset[Row] ={
    import spark.implicits._

    val spikeChannelsDataset = metaDataset.filter($"IS_EVENT".isNull && $"FileInfo".contains("Spike"))
      .select($"_id", $"HDFS_PATH", $"FileInfo")

    val sessions = spikeChannelsDataset.select($"FileInfo").dropDuplicates.collect.map(_.get(0).toString)
    val sessionsList = sessions.map(x => x.concat(", "))
      .foldLeft("[")((x, y) => x + y)
      .foldRight("]")((x, y) => x + y)
      .replace(", ]", "]")

    if(sessions.length > 1){
      logger.info(s"Sessions List: $sessionsList")

      sessions.map(s => {
        logger.info(s"Start Sorting $s spike trains")

        val channesBaseDir = spikeChannelsDataset.filter($"FileInfo" === s)
          .select("HDFS_PATH")
          .collect()
          .map(_.getAs[String](0))
          .toSeq

        val channelDataset = sparkReader.channelParquetReader(spark, channesBaseDir).persist()
        val sortedData = simpleSorting(spark, channelDataset, s)
        channelDataset.unpersist()

        sortedData

      }).reduce((df1, df2) => df1.union(df2))
        .withColumnRenamed("FileInfo", "SignalInfo")
        .withColumn("SessionOrExperiment", lit(experimentName))

    } else {
      val session = sessions.apply(0)
      logger.info(s"Start Sorting $session session")

      val channesBaseDir = spikeChannelsDataset.filter($"FileInfo" === session)
        .select("HDFS_PATH")
        .collect()
        .map(_.getAs[String](0))
        .toSeq

      val channelDataset = sparkReader.channelParquetReader(spark, channesBaseDir).persist()
      val sortedData = simpleSorting(spark, channelDataset, session)
      channelDataset.unpersist()

      sortedData.withColumnRenamed("FileInfo", "SignalInfo")
        .withColumn("SessionOrExperiment", lit(experimentName))

    }
  }


}
