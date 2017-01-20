/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.graph

import com.typesafe.scalalogging.slf4j.Logger
import org.apache.spark.sql.DataFrame
import org.graphframes.GraphFrame
import org.librairy.modeler.lda.models.Path
import org.slf4j.LoggerFactory
/**
  * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
  */
object DiscoveryPath {

  val logger = Logger(LoggerFactory.getLogger(DiscoveryPath.getClass))

  def apply (startUri: List[String], endUri: List[String], minScore: Double, maxLength: Integer, reltype: List[String], vertices: DataFrame, edges: DataFrame, maxResults: Integer) : Array[Path]={

    // Create a Vertex DataFrame with unique ID column "id"
    val v = vertices.toDF("id");

    // Create an Edge DataFrame with "src" and "dst" columns
    val e = edges.toDF("src","dst","score","type")

    // Create a GraphFrame
    val g = GraphFrame(v,e)

    val fromExpression = startUri.map(uri => "id='"+uri+"'").mkString(" or ")
    logger.info("From expression: " + fromExpression)

    val toExpression = endUri.map(uri => "id='"+uri+"'").mkString(" or ")
    logger.info("To expression: " + toExpression)

    // bfs
    val bfs = g.bfs.fromExpr(fromExpression).toExpr(toExpression);
    // filter by similarity score
    var edgeFilter = "score > " + minScore

    if (reltype != null && !reltype.isEmpty){
      val types = reltype.map(t => "type='"+t+"'").mkString(" or ")
      edgeFilter += " and (" + types + ")"
    }
    logger.info("edge filter: " + edgeFilter)
    bfs.edgeFilter(edgeFilter)
    // filter by max edges
    bfs.maxPathLength(maxLength)
    // calculate
    logger.info("getting shortest path between: " + startUri + " and " + endUri + " ...")
    val result = bfs.run().cache();

    result.show();

    //TODO handle direct path

    val numNodes = result.columns.filter( _.contains("v")).length

    val paths : Array[Path] = result.rdd.map(row => PathBuilder.apply(row,numNodes)).sortBy( _.getAccScore, false).take(maxResults)
    logger.debug("Paths: " + paths)

    return paths;
//    var path = new Path();
//
//    if (!paths.isEmpty){
//      path = paths(0);
//    }
//
//    return path
  }




}
