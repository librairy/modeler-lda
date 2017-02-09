/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.graph;

import es.cbadenes.lab.test.IntegrationTest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.dao.SimilaritiesDao;
import org.librairy.modeler.lda.models.Node;
import org.librairy.modeler.lda.models.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Category(IntegrationTest.class)
public class DiscoveryPathTest {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryPathTest.class);

    private SQLContext sqlContext;
    private JavaSparkContext sc;

    @Before
    public void setup(){
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[*]");
        this.sc = new JavaSparkContext(conf);
        this.sqlContext = new org.apache.spark.sql.SQLContext(sc);
    }


    @Test
    public void shortestPath(){

        // Vertices
        List<Row> uris = Arrays.asList(new Row[]{
                RowFactory.create("uri1"),
                RowFactory.create("uri2"),
                RowFactory.create("uri3"),
                RowFactory.create("uri4"),
                RowFactory.create("uri5"),
                RowFactory.create("uri6"),
                RowFactory.create("uri7"),
                RowFactory.create("uri8"),
                RowFactory.create("uri9"),
                RowFactory.create("uri10")
        });
        StructType shapeSchema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType, false)
                });

        // Edges
        List<Row> similarities = Arrays.asList(new Row[]{
                RowFactory.create("uri1","uri3",0.5,"item"),
                RowFactory.create("uri3","uri6",0.5,"part"),
                RowFactory.create("uri1","uri4",0.8,"part"),
                RowFactory.create("uri4","uri7",0.5,"item"),
                RowFactory.create("uri4","uri6",0.9,"part"),
                RowFactory.create("uri2","uri4",0.5,"part"),
                RowFactory.create("uri4","uri5",0.5,"item"),
                RowFactory.create("uri4","uri3",0.8,"item"),
                RowFactory.create("uri5","uri7",0.5,"item")
        });
        StructType similaritySchema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_1, DataTypes.StringType, false),
                        DataTypes.createStructField(SimilaritiesDao.RESOURCE_URI_2, DataTypes.StringType, false),
                        DataTypes.createStructField(SimilaritiesDao.SCORE, DataTypes.DoubleType, false),
                        DataTypes.createStructField("type", DataTypes.StringType, false),
                });

        DataFrame vertices  = this.sqlContext.createDataFrame(uris, shapeSchema);
        DataFrame edges     = this.sqlContext.createDataFrame(similarities, similaritySchema);;


        Double minScore     = 0.1;
        Integer maxLength   = 10;
        Integer maxResults  = 10;
        List<String> startUris  = Arrays.asList(new String[]{"uri6"});
        List<String> endUris    = Arrays.asList(new String[]{"uri3"});

        //List<String> types      = Arrays.asList(new String[]{"item","part"});
        List<String> types      = Collections.emptyList();

        Path[] paths = DiscoveryPath.apply(
                JavaConversions.asScalaBuffer(startUris).toList(),
                JavaConversions.asScalaBuffer(endUris).toList(),
                minScore,
                maxLength,
                JavaConversions.asScalaBuffer(types).toList(),
                vertices,
                edges,
                maxResults);


        if (paths == null || paths.length == 0){
            LOG.info("No paths found!");
            return;
        }

        for (Path path: paths){
            LOG.info("==========================");
            LOG.info("Accumulated Score :: " + path.getAccScore());
            LOG.info("Averaged Score :: " + path.getAvgScore());
            for (Node node : path.getNodes()){
                LOG.info("\t> " + node.toString());
            }
        }


    }
}
