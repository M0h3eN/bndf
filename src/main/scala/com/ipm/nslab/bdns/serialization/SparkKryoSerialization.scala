package com.ipm.nslab.bdns.serialization

import com.esotericsoftware.kryo.Kryo
import com.ipm.nslab.bdns.commons.io.{SparkReader, SparkWriter}
import com.ipm.nslab.bdns.commons.{FileSystem, MongoConnector}
import com.ipm.nslab.bdns.extendedTypes.{BICValues, ChannelMeta, ExperimentMetaDataEvaluator, Median, PathPropertiesEvaluator, RootPathPropertiesEvaluator}
import com.ipm.nslab.bdns.spark.DataIngestion
import com.ipm.nslab.bdns.spark.analysis.{GoodnessOfFit, Sorting}
import com.ipm.nslab.bdns.spark.commons.Transformers
import com.ipm.nslab.bdns.structure.{CellStructure, CharArrayStructure, CharStructure, DataStructure, EventStructure, MatrixStructure, SchemaCreator, StructStructure}
import org.apache.log4j.Logger
import org.apache.spark.serializer.KryoRegistrator

class SparkKryoSerialization extends KryoRegistrator {

  val logger: Logger = Logger.getLogger(s"${this.getClass.getName}")

  override def registerClasses(kryo: Kryo) {

    def registerByName(kryo: Kryo, name: String) {
      try {
        kryo.register(Class.forName(name))
      } catch {
        case cnfe: java.lang.ClassNotFoundException => {
          logger.debug("Could not register class %s by name".format(name))
        }
      }
    }

    // java.lang
    kryo.register(classOf[java.lang.Class[_]])

    // java.util
    kryo.register(classOf[java.util.ArrayList[_]])
    kryo.register(classOf[java.util.LinkedHashMap[_, _]])
    kryo.register(classOf[java.util.LinkedHashSet[_]])
    kryo.register(classOf[java.util.HashMap[_, _]])
    kryo.register(classOf[java.util.HashSet[_]])

    // org.apache.avro
    registerByName(kryo, "org.apache.avro.Schema$RecordSchema")
    registerByName(kryo, "org.apache.avro.Schema$Field")
    registerByName(kryo, "org.apache.avro.Schema$Field$Order")
    registerByName(kryo, "org.apache.avro.Schema$UnionSchema")
    registerByName(kryo, "org.apache.avro.Schema$Type")
    registerByName(kryo, "org.apache.avro.Schema$LockableArrayList")
    registerByName(kryo, "org.apache.avro.Schema$BooleanSchema")
    registerByName(kryo, "org.apache.avro.Schema$NullSchema")
    registerByName(kryo, "org.apache.avro.Schema$StringSchema")
    registerByName(kryo, "org.apache.avro.Schema$IntSchema")
    registerByName(kryo, "org.apache.avro.Schema$FloatSchema")
    registerByName(kryo, "org.apache.avro.Schema$EnumSchema")
    registerByName(kryo, "org.apache.avro.Schema$Name")
    registerByName(kryo, "org.apache.avro.Schema$LongSchema")
    registerByName(kryo, "org.apache.avro.generic.GenericData$Array")

    //com.ipm.nslab.bdns

    //com.ipm.nslab.bdns.commons
    kryo.register(classOf[FileSystem])
    kryo.register(classOf[MongoConnector])
    kryo.register(classOf[SparkReader])
    kryo.register(classOf[SparkWriter])
    //com.ipm.nslab.bdns.extendedTypes
    kryo.register(classOf[ExperimentMetaDataEvaluator])
    kryo.register(classOf[PathPropertiesEvaluator])
    kryo.register(classOf[RootPathPropertiesEvaluator])
    kryo.register(classOf[Median])
    kryo.register(classOf[BICValues])
    kryo.register(classOf[ChannelMeta])
    //com.ipm.nslab.bdns.spark
    kryo.register(classOf[DataIngestion])
    kryo.register(classOf[GoodnessOfFit])
    kryo.register(classOf[Sorting])
    kryo.register(classOf[Transformers])
    //com.ipm.nslab.bdns.structure
    kryo.register(classOf[CellStructure])
    kryo.register(classOf[CharArrayStructure])
    kryo.register(classOf[CharStructure])
    kryo.register(classOf[DataStructure])
    kryo.register(classOf[EventStructure])
    kryo.register(classOf[MatrixStructure])
    kryo.register(classOf[SchemaCreator])
    kryo.register(classOf[StructStructure])


    // org.apache.spark.internal
    registerByName(kryo, "org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage")

    // org.apache.spark.catalyst
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow])

    // org.apache.spark.sql
    registerByName(kryo, "org.apache.spark.sql.execution.datasources.FileFormatWriter$WriteTaskResult")
    registerByName(kryo, "org.apache.spark.sql.execution.datasources.BasicWriteTaskStats")
    registerByName(kryo, "org.apache.spark.sql.execution.datasources.ExecutedWriteSummary")
    registerByName(kryo, "org.apache.spark.sql.execution.datasources.WriteTaskResult")
    registerByName(kryo, "org.apache.spark.sql.types.BooleanType$")
    registerByName(kryo, "org.apache.spark.sql.types.DoubleType$")
    registerByName(kryo, "org.apache.spark.sql.types.FloatType$")
    registerByName(kryo, "org.apache.spark.sql.types.IntegerType$")
    registerByName(kryo, "org.apache.spark.sql.types.LongType$")
    registerByName(kryo, "org.apache.spark.sql.types.StringType$")
    kryo.register(classOf[org.apache.spark.sql.types.ArrayType])
    kryo.register(classOf[org.apache.spark.sql.types.MapType])
    kryo.register(classOf[org.apache.spark.sql.types.Metadata])
    kryo.register(classOf[org.apache.spark.sql.types.StructField])
    kryo.register(classOf[org.apache.spark.sql.types.StructType])

    // scala
    kryo.register(classOf[scala.Array[scala.Array[Byte]]])
    kryo.register(classOf[scala.Array[java.lang.Integer]])
    kryo.register(classOf[scala.Array[java.lang.Long]])
    kryo.register(classOf[scala.Array[java.lang.Object]])
    kryo.register(classOf[scala.Array[org.apache.spark.sql.catalyst.InternalRow]])
    kryo.register(classOf[scala.Array[org.apache.spark.sql.types.StructField]])
    kryo.register(classOf[scala.Array[org.apache.spark.sql.types.StructType]])
    kryo.register(classOf[scala.Array[scala.collection.Seq[_]]])
    kryo.register(classOf[scala.Array[Int]])
    kryo.register(classOf[scala.Array[Long]])
    kryo.register(classOf[scala.Array[String]])
    kryo.register(classOf[scala.Array[Option[_]]])
    registerByName(kryo, "scala.Tuple2$mcCC$sp")

    // scala.collection
    registerByName(kryo, "scala.collection.Iterator$$anon$11")
    registerByName(kryo, "scala.collection.Iterator$$anonfun$toStream$1")

    // scala.collection.convert
    registerByName(kryo, "scala.collection.convert.Wrappers$")

    // scala.collection.immutable
    kryo.register(classOf[scala.collection.immutable.::[_]])
    kryo.register(classOf[scala.collection.immutable.Range])
    registerByName(kryo, "scala.collection.immutable.Stream$Cons")
    registerByName(kryo, "scala.collection.immutable.Stream$Empty$")
    registerByName(kryo, "scala.collection.immutable.Set$EmptySet$")

    // scala.collection.mutable
    kryo.register(classOf[scala.collection.mutable.ArrayBuffer[_]])
    kryo.register(classOf[scala.collection.mutable.ListBuffer[_]])
    registerByName(kryo, "scala.collection.mutable.ListBuffer$$anon$1")
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofInt])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofLong])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofByte])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofChar])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])

    // scala.math
    kryo.register(scala.math.Numeric.LongIsIntegral.getClass)

    // scala.reflect
    registerByName(kryo, "scala.reflect.ClassTag$GenericClassTag")

    // This seems to be necessary when serializing a RangePartitioner, which writes out a ClassTag:
    //
    //  https://github.com/apache/spark/blob/v1.5.2/core/src/main/scala/org/apache/spark/Partitioner.scala#L220
    //
    // See also:
    //
    //   https://mail-archives.apache.org/mod_mbox/spark-user/201504.mbox/%3CCAC95X6JgXQ3neXF6otj6a+F_MwJ9jbj9P-Ssw3Oqkf518_eT1w@mail.gmail.com%3E
    registerByName(kryo, "scala.reflect.ClassTag$$anon$1")

    // needed for manifests
    registerByName(kryo, "scala.reflect.ManifestFactory$ClassTypeManifest")

    // Added to Spark in 1.6.0; needed here for Spark < 1.6.0.
    kryo.register(classOf[Array[Tuple1[Any]]])
    kryo.register(classOf[Array[(Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])

    kryo.register(Map.empty.getClass)
    kryo.register(Nil.getClass)
    kryo.register(None.getClass)
  }
}
