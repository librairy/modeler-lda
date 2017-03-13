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
import org.librairy.modeler.lda.models.{Node, Path}
import org.slf4j.LoggerFactory

/**
  * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
  */
object PregelSSSP {

  val logger = Logger(LoggerFactory.getLogger(PregelSSSP.getClass))

  def apply (startUri: List[String], endUri: List[String], minScore: Double, maxLength: Integer, reltype: List[String], vertices: DataFrame, edges: DataFrame, maxResults: Integer, partitions: Integer) : Array[Path]={


    val nodes: RDD[(VertexId, (String,String))] = vertices.rdd.map(row => (row.getString(0).hashCode.toLong,(row.getString(0),row.getString(1))))

//    logger.info("nodes: " + nodes.collect().mkString("\n"))

    val relationships: RDD[Edge[Double]] =  edges.rdd.map(row => Edge(row.getString(0).hashCode.toLong, row.getString(1).hashCode.toLong, row.getDouble(2)))

//    logger.info("relationships: " + relationships.collect().mkString("\n"))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("Missing","item")

    // Build the initial Graph
    val graph : Graph[(String, String), Double] = Graph(nodes, relationships, defaultUser)

    val v2 = endUri(0).hashCode.toLong

    logger.info("End node ID: " + v2)

    val v1 = startUri(0).hashCode.toLong

    logger.info("Start node ID: " + v1)

    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph : Graph[(Double, List[VertexId]), Double] = graph.mapVertices((id, _) => if (id == v1) (0.0, List[VertexId](v1)) else (Double.PositiveInfinity, List[VertexId]()))


    val sssp = initialGraph.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out)(

      // Vertex Program
      (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist,

      // Send Message
      triplet => {
        if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr ) {
          Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr , triplet.srcAttr._2 :+ triplet.dstId)))
        } else {
          Iterator.empty
        }
      },
      //Merge Message
      (a, b) => if (a._1 < b._1) a else b)

    var path = new Path();


    val partialResult =sssp.vertices.filter( res => res._1 == v2)

    if (partialResult.isEmpty()) return Array.empty[Path];

    val result = partialResult.sortBy(t => t._2._1, false, partitions).first()._2
    logger.info("Accumulated Score: " + result._1)

    if (result._1 == Double.PositiveInfinity) return Array.empty[Path];

    result._2.foreach( v => path.add(new Node(nodes.filter( t => t._1 == v).first()._2._1,0.0)))

    logger.debug("Pregel result: " + sssp.vertices.collect.mkString("\n"))

    return Array(path)
  }

}
