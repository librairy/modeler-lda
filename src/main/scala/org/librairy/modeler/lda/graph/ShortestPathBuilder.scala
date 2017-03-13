/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.graph

import com.typesafe.scalalogging.slf4j.Logger
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.librairy.modeler.lda.models.Path
import org.slf4j.LoggerFactory

/**
  * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
  */
object ShortestPathBuilder {

  val logger = Logger(LoggerFactory.getLogger(DiscoveryPath.getClass))

  def apply (startUri: List[String], endUri: List[String], minScore: Double, maxLength: Integer, reltype: List[String], vertices: DataFrame, edges: DataFrame, maxResults: Integer, partitions: Integer) : Array[Path]={


    val nodes: RDD[(VertexId, (String,String))] = vertices.rdd.map(row => (row.getString(0).hashCode.toLong,(row.getString(0),row.getString(1))))

//    logger.info("nodes: " + nodes.collect().mkString("\n"))

    val relationships: RDD[Edge[String]] =  edges.rdd.map(row => Edge(row.getString(0).hashCode.toLong, row.getString(1).hashCode.toLong, row.getDouble(2).toString))

//    logger.info("relationships: " + relationships.collect().mkString("\n"))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("Missing","item")

    // Build the initial Graph
    val graph = Graph(nodes, relationships, defaultUser)

    val v2 = endUri(0).hashCode.toLong

    logger.info("end node: " + v2)

    val v1 = startUri(0).hashCode.toLong

    logger.info("start node: " + v1)

    val result = ShortestPaths.run(graph, Seq(v2))

    val shortestPath = result               // result is a graph
      .vertices                             // we get the vertices RDD
      .filter({case(vId, _) => vId == v1})  // we filter to get only the shortest path from v1
      .first                                // there's only one value
      ._2                                   // the result is a tuple (v1, Map)
      .get(v2)


//    logger.info("result: " + result.vertices.collect().mkString("\n"))

//    logger.info("route: " + result.edges.collect().mkString("\n"))

    val path : ShortestPaths.SPMap =  result.vertices.filter( {case (vId,_) => vId == v1} ).first()._2;

    val numSteps = path.get(v2).get

    logger.info("Shortest path steps: " + numSteps)

    return Array()
  }

}
