package com.ipm.nslab.bdns.spark.analysis

import com.ipm.nslab.bdns.commons.io.SparkReader
import com.ipm.nslab.bdns.extendedTypes.{BICValues, ChannelMeta, Median}
import com.ipm.nslab.bdns.spark.commons.Transformers
import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{abs, col, collect_list, count, explode, lag, lit, mean, monotonically_increasing_id, sequence, sort_array, sqrt, stddev, struct, when}
import org.apache.spark.sql.expressions.Window

class Sorting {

  val transformers = new Transformers
  val goodnessOfFit = new GoodnessOfFit
  val sparkReader = new SparkReader

  val logger: Logger = Logger(s"${this.getClass.getName}")

  def getSignalSummaryDataset(spark: SparkSession, ChannelDataset: Dataset[Row], fileInfo: String): Dataset[Row] ={
    import spark.implicits._

    val med = ChannelDataset.filter($"fileInfo" === fileInfo)
                     .select(abs($"signal").alias("signal"))
                     .stat.approxQuantile("signal", Array(0.5), 0.00001).apply(0)

    val medianDataset = Seq(Median(fileInfo, med)).toDF()

    val summaryDataset = ChannelDataset
      .groupBy($"FileInfo")
      .agg(mean($"signal").alias("mean"), stddev($"signal").alias("se"), count($"signal").alias("n"))
      .withColumn("LB", $"mean" - lit(2) * $"se"/sqrt($"n"))
      .withColumn("UB", $"mean" + lit(2) * $"se"/sqrt($"n"))
      .join(medianDataset, Seq("FileInfo"), "left")
      .withColumn("threshold", lit(3) * $"median"/0.6745)

    summaryDataset
  }

  def getWindowedSpikeDataset(spark: SparkSession, channelDataset: Dataset[Row], fileInfo: String): Dataset[Row] ={
    import spark.implicits._

    val overFileInfo = Window.partitionBy("FileInfo").orderBy("Time")

    // Threshold options:
    // 1- signal < -t
    // 1- signal > t
    // 1- (signal < -t) || (signal > t)

    val summaryDataset = getSignalSummaryDataset(spark, channelDataset, fileInfo)
    val thresholdedDataset = channelDataset.join(summaryDataset, Seq("FileInfo"), "left")
    .withColumn("Spike", when($"signal" < -$"threshold", 1).otherwise(0))
    .select($"FileInfo", $"Time", $"Signal", $"Spike")

    val windowdSpikeDataset = thresholdedDataset.filter($"Spike" === 1)
      .select($"FileInfo", $"Signal", $"Spike", $"Time", lag($"Time", 1) over overFileInfo as "LAG")
      .withColumn("LAG", when($"LAG".isNull, 0).otherwise($"LAG"))
      .withColumn("diff", $"Time" - $"LAG")
      .filter($"diff" =!= 1)
      .withColumn("lwb", $"Time" - 30)
      .withColumn("uwb", $"Time" + 50)
      .withColumn("SpikeWindow", sequence($"lwb", $"uwb"))
      .withColumn("SparkSetNumber", monotonically_increasing_id)
      .select("FileInfo", "SpikeWindow", "SparkSetNumber")
      .withColumn("Time", explode($"SpikeWindow"))
      .drop("SpikeWindow")

    windowdSpikeDataset

  }

  def getMlTransformedColumnDataset(spark: SparkSession, channelDataset: Dataset[Row], fileInfo: String): Dataset[Row] ={
    import spark.implicits._

    val windowedSpikeDataset = getWindowedSpikeDataset(spark, channelDataset, fileInfo)
    val mlTransformedDataset = windowedSpikeDataset.join(channelDataset, Seq("FileInfo", "Time"), "left")
      .groupBy("channelName", "FileInfo", "SparkSetNumber")
      .agg(sort_array(collect_list(struct("Time", "Signal", "EventCode").alias("SpikeInfo"))).alias("SpikeInfo"))
      .withColumn("SpikeSignals", transformers.arrayToVectorUDF($"SpikeInfo.Signal"))

    mlTransformedDataset

  }

  def simpleSorting(spark: SparkSession, channelDataset: Dataset[Row], fileInfo: String) :Dataset[Row] ={

    val mlTransformedDataset = getMlTransformedColumnDataset(spark, channelDataset, fileInfo).persist()

    val pca = new PCA()
      .setInputCol("SpikeSignals")
      .setOutputCol("pcaFeatureColumn")
      .setK(6)
      .asInstanceOf[PipelineStage]

    val minK = 2
    val maxK = 6

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

    // get best model based on BIC

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

  def sorter(spark: SparkSession, metaDataset: Dataset[Row], experimentName: String): Dataset[Row] ={
    import spark.implicits._

    val spikeChannelsDataset = metaDataset.filter($"IS_EVENT".isNull && $"FileInfo".contains("Spike"))
      .select($"_id", $"HDFS_PATH", $"FileInfo")

    val sessions = spikeChannelsDataset.select($"FileInfo").dropDuplicates.collect.map(_.get(0).toString)

    if(sessions.length > 1){

      sessions.map(s => {
        logger.info(s"Start Sorting $s spike trains")

        val channelInfoMap = spikeChannelsDataset.filter($"FileInfo" === s)
          .drop("FileInfo")
          .collect
          .map(x => ChannelMeta(x.getAs("_id").toString, x.getAs("HDFS_PATH").toString))

        val channelDataset = channelInfoMap.map(channel => {
          sparkReader.channelParquetReader(spark, channel)
        }).reduce((df1, df2) => df1.union(df2))

        val sortedData = simpleSorting(spark, channelDataset, s)
        sortedData

      }).reduce((df1, df2) => df1.union(df2))
        .withColumnRenamed("FileInfo", "RecordLocation")
        .withColumn("SessionOrExperiment", lit(experimentName))

    } else {
      val session = sessions.apply(0)
      logger.info(s"Start Sorting $session session")

      val channelInfoMap = spikeChannelsDataset.filter($"FileInfo" === session)
        .drop("FileInfo")
        .collect
        .map(x => ChannelMeta(x.getAs("_id").toString, x.getAs("HDFS_PATH").toString))

      val channelDataset = channelInfoMap.map(channel => {
        sparkReader.channelParquetReader(spark, channel)
      }).reduce((df1, df2) => df1.union(df2))

      val sortedData = simpleSorting(spark, channelDataset, session)
      sortedData.withColumnRenamed("FileInfo", "RecordLocation")
        .withColumn("SessionOrExperiment", lit(experimentName))

    }
  }


}
