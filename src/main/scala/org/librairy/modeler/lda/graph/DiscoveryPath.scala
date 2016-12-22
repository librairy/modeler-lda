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

  def apply (startUri: String, endUri: String, minScore: Double, maxLength: Integer, vertices: DataFrame, edges: DataFrame) : Path={

    // Create a Vertex DataFrame with unique ID column "id"
    val v = vertices.toDF("id");

    // Create an Edge DataFrame with "src" and "dst" columns
    val e = edges.toDF("src","dst","score")

    // Create a GraphFrame
    val g = GraphFrame(v,e)

    // bfs //TODO more than one src and more than one dst
    val bfs = g.bfs.fromExpr("id = '" + startUri+"'").toExpr("id = '"+endUri+"'");
    // filter by similarity score
    bfs.edgeFilter("score > " + minScore)
    // filter by max edges
    bfs.maxPathLength(maxLength)
    // calculate
    logger.info("analyzing graph..")
    val result = bfs.run().cache();

    result.show();

    //TODO handle direct path

    val numNodes = result.columns.filter( _.contains("v")).length

    val paths : Array[Path] = result.rdd.map(row => PathBuilder.apply(row,numNodes)).sortBy( _.getAccumulatedScore, false).take(1)
    logger.debug("Paths: " + paths)

    var path = new Path();

    if (!paths.isEmpty){
      path = paths(0);
    }

    return path
  }




}
