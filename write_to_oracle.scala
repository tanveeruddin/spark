-- it was compllaining abt jdbc driver not found. after copying the jdbc jar file, it worked. 
-- for string datatype, it creates default 255 char. Tried lots of things e.g. MetadataBuilder,withColumn  etc. finally specifying createTableColumnTypes worked.
https://github.com/databricks/spark-redshift#configuring-the-maximum-size-of-string-columns
https://lostechies.com/ryansvihla/2016/04/10/connection-to-oracle-from-spark/
-- ============
--option("mode", "DROPMALFORMED")

val df=spark.read.option("inferSchema", "true").option("quote", "\"").option("mode", "DROPMALFORMED").option("header", "true").csv("c:\\mhl\\ReportResult_20Nov2018.csv_1339474") 

--drop unnecessary columns
val df1= df.drop("Content Type","Starting URL", "Description","Objectives","Keywords","Audience","SPARE57","SPARE58","SPARE59","SPARE60","CONTENT OBJECT A3","CONTENT OBJECT A4","CONTENT OBJECT A5","CONTENT OBJECT A6","CONTENT OBJECT A7","CONTENT OBJECT A8","CONTENT OBJECT A9","CONTENT OBJECT A10","CONTENT OBJECT A11","CONTENT OBJECT A12","CONTENT OBJECT A13","CONTENT OBJECT A14","CONTENT OBJECT A15","CONTENT OBJECT A16","CONTENT OBJECT A17","CONTENT OBJECT A18","CONTENT OBJECT A19","CONTENT OBJECT A20","CONTENT OBJECT A21","CONTENT OBJECT A22","CONTENT OBJECT A23","CONTENT OBJECT A24","CONTENT OBJECT A25","CONTENT OBJECT A26","CONTENT OBJECT A27","CONTENT OBJECT A28","CONTENT OBJECT A29","CONTENT OBJECT A30","CONTENT OBJECT A31","CONTENT OBJECT A32","CONTENT OBJECT A33","CONTENT OBJECT A34","CONTENT OBJECT A35","CONTENT OBJECT A36","CONTENT OBJECT A37","CONTENT OBJECT A38","CONTENT OBJECT A39","CONTENT OBJECT A40")

--.option("createTableColumnTypes", "Objectives VARCHAR(2048), Keywords VARCHAR(2048), Audience VARCHAR(2048)")

df1.write
.format("jdbc")
.option("url", "jdbc:oracle:thin:hr/hr@//10.10.10.10:1522/my_oracle_instance")
.option("dbtable", "ReportResult")
.option("user", "hr")
.option("password", "hr")
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("mode","append")
.save;


/*
import org.apache.spark.sql.types.MetadataBuilder
val columnLengthMap = Map(
  "Description" -> 1024,
  "Objectives" -> 1024,
  "Keywords" -> 1024,
  "Audience" -> 1024
)

--val df2 = df1.withColumn("Description_",df1("Description").cast(org.apache.spark.sql.types.VarcharType(1000)))

// Apply each column metadata customization
columnLengthMap.foreach { case (colName, length) =>
  var metadata = new MetadataBuilder().putLong("maxlength", length).build()
  df1 = df1.withColumn(colName, col(colName).as(colName, metadata))
}

*/

