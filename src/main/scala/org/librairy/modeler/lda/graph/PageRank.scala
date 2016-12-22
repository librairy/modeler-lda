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
import org.slf4j.LoggerFactory

/**
  * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
  */
object PageRank {

  val logger = Logger(LoggerFactory.getLogger(PageRank.getClass))

  def apply (startUri: String, endUri: String, minScore: Double, maxLength: Integer, vertices: DataFrame, edges: DataFrame) : String={

    // Create a Vertex DataFrame with unique ID column "id"
    val v = vertices.toDF("id");

    // Create an Edge DataFrame with "src" and "dst" columns
    val e = edges.toDF("src","dst","score")

    // Create a GraphFrame
    val g = GraphFrame(v,e);

    // pagerank
    val results = g.pageRank.resetProbability(0.01).maxIter(20).run();
    results.vertices.select("id","pagerank").show();

    "done"
  }




}
