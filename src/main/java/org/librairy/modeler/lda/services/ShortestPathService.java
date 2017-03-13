/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.services;

import com.google.common.base.Strings;
import org.apache.spark.sql.DataFrame;
import org.librairy.modeler.lda.api.LDAModelerAPI;
import org.librairy.modeler.lda.graph.DiscoveryPath;
import org.librairy.modeler.lda.graph.PregelSSSP;
import org.librairy.modeler.lda.models.Node;
import org.librairy.modeler.lda.models.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.collection.JavaConversions;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class ShortestPathService {

    private static final Logger LOG = LoggerFactory.getLogger(ShortestPathService.class);

    @Autowired
    LDAModelerAPI api;

    public Path[] calculate(String domainUri, List<String> startUris, List<String> endUris, List<String> resTypes, Double minScore, Integer maxLength, DataFrame shapesDF, DataFrame similaritiesDF, Integer maxResults, Integer partitions, Boolean intermediateScore){

        scala.collection.immutable.List<String> start   = JavaConversions.asScalaBuffer(startUris).toList();
        scala.collection.immutable.List<String> end     = JavaConversions.asScalaBuffer(endUris).toList();
        scala.collection.immutable.List<String> types   = JavaConversions.asScalaBuffer(resTypes).toList();

        Instant s1 = Instant.now();
        Path[] paths = byPregelSSSP(domainUri, start, end, minScore, maxLength, types, shapesDF, similaritiesDF, maxResults, partitions, intermediateScore);
//        Path[] paths = byBFS(start, end, minScore, maxLength, types, shapesDF, similaritiesDF, maxResults, partitions);
        LOG.info("Shortest-Path Elapsed time: " + Duration.between(s1, Instant.now()).toMillis() + " msecs");


        shapesDF.unpersist();
        similaritiesDF.unpersist();

       return paths;

    }

    private Path[] byPregelSSSP(String domainUri, scala.collection.immutable.List<String> start, scala.collection.immutable.List<String> end, Double minScore, Integer maxLength, scala.collection.immutable.List<String> types, DataFrame shapesDF, DataFrame similaritiesDF, Integer maxResults, Integer partitions, Boolean intermediateScore){
        Path[] paths = PregelSSSP.apply(start, end, minScore, maxLength, types, shapesDF, similaritiesDF, maxResults, partitions);

        // Load intermediate similarities


        if (intermediateScore && (paths != null) && (paths.length > 0)){
            Path path = paths[0];

            String previousUri = "";
            for (Node node : path.getNodes()){
                if (Strings.isNullOrEmpty(previousUri)){
                    node.setScore(1.0);
                }else{
                    Double score = api.compare(previousUri, node.getUri(), domainUri);
                    node.setScore(score);
                }
                previousUri = node.getUri();
            }
        }

        return paths;
    }

    private Path[] byBFS(scala.collection.immutable.List<String> start, scala.collection.immutable.List<String> end, Double minScore, Integer maxLength, scala.collection.immutable.List<String> types, DataFrame shapesDF, DataFrame similaritiesDF, Integer maxResults, Integer partitions){
        Path[] paths = DiscoveryPath.apply(start, end, minScore, maxLength, types, shapesDF, similaritiesDF, maxResults, partitions);
        return paths;
    }


}
