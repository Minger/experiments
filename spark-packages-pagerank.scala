/*
* r
*/
object Cells {
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.graphx.GraphLoader
  import scala.util.hashing.{MurmurHash3=>MH3}
  import org.apache.spark.graphx.Graph
  import org.apache.spark.rdd.RDD
  import org.apache.spark.graphx._
  import org.apache.spark.sql.functions._
  
  val session = org.apache.spark.sql.SparkSession.builder
          .master("local")
          .appName("Spark CSV Reader")
          .getOrCreate;
  
  val df = session.read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("mode", "DROPMALFORMED")
          .csv("/opt/docker/hostdir/requirements.csv");
  
  val package_names = df.select("package_name").distinct().na.drop();
  
  val packages: RDD[(VertexId, String)] = package_names.rdd
    .map{row: Row => 
      val package_name = row.getString(0)                                                  
      (MH3.stringHash(package_name), package_name)
    }
  
  val edges:RDD[Edge[String]] = df.na.drop()
    .rdd
    .map { row: Row =>
      val requirement = MH3.stringHash(row.getString(1))
      val package_name = MH3.stringHash(row.getString(2))
      Edge(package_name, requirement, "requires")
    }
  
  val defaultPackage = "no-package"

  val graph = Graph(packages, edges, defaultPackage)

  val packageRankings = graph.pageRank(0.1).vertices
  
  val ranksAndPackageNames = packageRankings
    .join(packages)
    .sortBy(_._2._1, ascending=false)
    .map(x=>(x._2._1,x._2._2))

  ranksAndPackageNames.take(100)
}
                  