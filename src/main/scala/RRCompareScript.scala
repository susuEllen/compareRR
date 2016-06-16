import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, _}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/*
* Automatic script to compare raw feature values between old and new RR
* AC: run it on SDE for VMWARE and WORKDAY. Generate list of features that are out-of-sync
* */
object RRCompareScript {

  val predictedValueIndex = 2

  def main(args: Array[String]) = {
    if(args.length < 2) {
      println("\n\nUsage:\n\tRRCompareScript <oldRRCSV> <newRRCSV>\n\te.g RRCompareScript oldRR_log.csv newRR_log.csv")
    } else {
      val oldRRCSV: File = loadCSV(args(0))
      val newRRCSV: File = loadCSV(args(1))

      val conf = new SparkConf().setAppName("RRCompareScript").setMaster("local")
      val sc = new SparkContext(conf)

      val oldRRResultArray = sc.textFile(oldRRCSV.getCanonicalPath)
      val newRRResultArray = sc.textFile(newRRCSV.getCanonicalPath)
      val headerOldRR: Array[String] = cleanHeader(oldRRResultArray.first())
      val headerNewRR: Array[String] = cleanHeader(newRRResultArray.first())

      println(s"\nOld RR headers\n\t${headerOldRR.sorted.mkString(" ~ ")}\n")
      println(s"\nNew RR headers\n\t${headerNewRR.sorted.mkString(" ~ ")}\n")
      assert(headerOldRR.length == headerNewRR.length, s"Mismatch in # of columns in old RR vs new RR. old: new --> ${headerOldRR.length} vs ${headerNewRR.length}")


      val sqlContext = new SQLContext(sc)
      val oldRRSchema: StructType = StructType(generateCustomSchema(headerOldRR))
      val newRRSchema: StructType = StructType(generateCustomSchema(headerNewRR))
      assert(oldRRSchema.fieldNames.sorted.sameElements(newRRSchema.fieldNames.sorted), s"Mismatch in old RR schema and new RR schema \noldRRSchema\n${oldRRSchema.printTreeString()} \nnewRRSchema\n ${newRRSchema.printTreeString()}")

      val rowRDD_oldRR: RDD[Row] = oldRRResultArray.zipWithIndex().filter(_._2 > 0).map(_._1.split(",")).map(Row.fromSeq(_))
      val rowRDD_newRR: RDD[Row] = newRRResultArray.zipWithIndex().filter(_._2 > 0).map(_._1.split(",")).map(Row.fromSeq(_))

      import  org.apache.spark.sql.functions.col
      val oldRRCol = oldRRSchema.fieldNames.map{ name => col(name)}

      val df_oldRR = sqlContext.createDataFrame(rowRDD_oldRR, oldRRSchema)
      val df_newRR = sqlContext.createDataFrame(rowRDD_newRR, newRRSchema)

      df_newRR.registerTempTable("newRR")
      //Use select to ensure df and df1 is in the correct order for comparison
      val df_newRRInOldRROrder = sqlContext.sql(s"SELECT ${oldRRSchema.fieldNames.mkString(",")} FROM newRR")


      val combinedDF: DataFrame = df_oldRR.join(df_newRRInOldRROrder, "id")
      println(s"\nCombined dataFrame size: ${combinedDF.count()}")
      println(s"\nCombined dataFrame schema:\n")
      combinedDF.printSchema()
      val columnSize = df_oldRR.columns.length
      val whatever = combinedDF.collect()
      val errorMessages = combinedDF.map(compareValues(_, columnSize)).collect()
      println(s"\n# of mismatch: ${errorMessages.count(!_.isEmpty)} \n")
      for (message <- errorMessages) {
        if(!message.isEmpty) {
          println(message)
        }
      }
      sc.stop()

    }
  }


  def generateCustomSchema(headerOldRR: Array[String]) = {
    //Seq(StructField("id", StringType, true), StructField("predictedLabel", StringType, true))
    headerOldRR.map{ fieldName =>
      val nullable: Boolean = (fieldName != "id" && fieldName != "predictedLabel" && fieldName != "predictedValue")
      StructField(fieldName, StringType, nullable)
    }
  }

  def compareValues(row: Row, compareStartIndex: Int): String = {
    var startIndex1 = 1
    var startIndex2 = compareStartIndex
    val buf = new StringBuilder

    while (startIndex1 < compareStartIndex) {
      val value1 = cleanValues(row(startIndex1).toString)
      val value2 = cleanValues(row(startIndex2).toString)
      if((startIndex1 == predictedValueIndex && doubleValueNotEqual(value1, value2)) ||
        (startIndex1 != predictedValueIndex  && !cleanValues(value1).equals(cleanValues(value2)))) {
        //TODO: also print in an output file
        buf.append(s"${row(0)} has mismatch field: ${row.schema.fieldNames(startIndex1)} \t$value1 vs $value2\n")
      }
      startIndex1 += 1
      startIndex2 += 1
    }
    buf.toString()
  }

  def doubleValueNotEqual(valueOld: String, valueNew:String) = {
    BigDecimal(valueOld.toDouble).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble !=
      BigDecimal(valueNew.toDouble).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  /*
  * clean Header Strings such that they should match between old and new RR
  * */
  def cleanHeader(header: String) = {
    header.toLowerCase.replace("featurevector(", "")
      .replace("others", "other")
      .replace(")","")
      .replace(".com.workday.sparkjobs.retentionrisk.featureextractor.","")
      .replace(" ","")
      .replace("$","")
      .split(",").map{ str => if(str.contains(":")) { str.split(":").last} else str}
  }

  /*
  * Clean values so that they can be compared between old and new RR
  * */
  def cleanValues(value: String) = {
    value.toLowerCase.replace("$", "").replace("(","").replace(")", "").replace("maplike","").replace("vector","").trim()
  }

  def loadCSV(csvFileName: String) = {
    val filePath: String = new File(s"./$csvFileName").getCanonicalPath
    if(!new File(filePath).exists()) {
      throw new RuntimeException(s"File: $filePath does not exist!")
    } else {
      new File(filePath)
    }
  }
}




