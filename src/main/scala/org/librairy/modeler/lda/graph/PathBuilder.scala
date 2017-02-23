/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.graph

import com.typesafe.scalalogging.slf4j.Logger
import org.apache.spark.sql.{DataFrame, Row}
import org.librairy.modeler.lda.models.Path
import org.librairy.modeler.lda.models.Node
import org.slf4j.LoggerFactory

/**
  * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
  */
object PathBuilder {

  val logger = Logger(LoggerFactory.getLogger(PathBuilder.getClass))

  def apply (row: Row, num: Int) : Path={

    logger.debug("Creating path from " + row)
    val path = new Path();

    // add first node
    val firstValue  = row.getStruct(0)
    val firstNode   = new Node(firstValue.getString(0), 1.0)
    path.add(firstNode)

    for (i <- 0 to num){
      val index = (i*2)+1
      val value = row.getStruct(index)
      val node = new Node(value.getString(1), value.getDouble(2));
      path.add(node)
    }

    logger.debug("Path created from " + row)
    return path
  }

}
