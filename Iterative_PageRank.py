import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

// Load the edges as a graph
val graph = GraphLoader.edgeListFile(sc, "data/edges.txt")
// Run Daynamic PageRank
val ranks = graph.pageRank(0.0001).vertices
// Join the ranks with the usernames
val users = sc.textFile("data/nodes.txt").map { line =>
  val fields = line.split(",")
  (fields(0).toLong, fields(1))
}
val ranksByUsername = users.join(ranks).map {
  case (id, (username, rank)) => (username, rank)
}
// Print the result
println(ranksByUsername.collect().mkString("\n"))

import scala.reflect.ClassTag

  def run[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int = 20, resetProb: Double = 0.15)
  {
      
      val outdegreeGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices( (id, attr) => 1.0 )
      .cache()

      outdegreeGraph.triplets.map(
         triplet => triplet.srcId + " sends prob=" + triplet.attr + " to " + triplet.dstId
         ).collect.foreach(println(_))  
      
      
    def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double ={
      resetProb + (1.0 - resetProb) * msgSum
    }
    def sendMessage(edge: EdgeTriplet[Double, Double]) =
      Iterator((edge.dstId, edge.srcAttr * edge.attr))
    def messageCombiner(a: Double, b: Double): Double = a + b
    val initialMessage = 0.0
      
    val pageRankGraph = outdegreeGraph.pregel(initialMessage, numIter, activeDirection = EdgeDirection.Out)(
      vertexProgram, sendMessage, messageCombiner)
      
   pageRankGraph.vertices.collect.foreach{
     case (vId, value) => println(s"${vId} has pagerank= ${value}")
    } 
  }

val graph = GraphLoader.edgeListFile(sc, "data/edges.txt")

val outDegrees: VertexRDD[Int] = graph.outDegrees
outDegrees.collect.foreach(a => println(a))
run(graph, 20, 0.15)

val iter=20
val lines = sc.textFile("data/wiki-Vote.txt") 
val edges = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
var ranks = edges.mapValues(v => 1.0)

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

var new_ranks = ranks

for (i <- 1 to iter) {
  new_ranks = new_ranks.join(edges).map{case(id,(rank,edges))=>(edges,rank/edges.size)} //Step_1:Outgoing_edges
  .flatMap{case(edges,rank)=>edges.map(e=>(e,rank))} //Step_2:Transitions
  .reduceByKey((v1,v2)=>v1+v2) //Step_3:Reduce By Key
  .mapValues(rank=>resetProb+(1-resetProb)*rank) //Step_4:Map_values
}

//<sort> and <display the top 10>
new_ranks.toDF("Edge","Rank").sort($"Rank".desc).show(10)
