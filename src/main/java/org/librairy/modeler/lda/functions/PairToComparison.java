/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.functions;

import org.librairy.metrics.data.Ranking;
import org.librairy.metrics.distance.ExtendedKendallsTauDistance;
import org.librairy.modeler.lda.models.Comparison;
import org.librairy.modeler.lda.models.Field;
import org.librairy.modeler.lda.utils.LevenshteinSimilarity;

import java.io.Serializable;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class PairToComparison implements Serializable {

//    public static void apply(){
//        Comparison<Field> topicComparison = new Comparison<Field>();
//
//        Field fieldOne = new Field();
//        fieldOne.setContainerUri(pair._1.getDescription());
//        fieldOne.setFieldUri(pair._1.getUri());
//        topicComparison.setFieldOne(fieldOne);
//
//        Field fieldTwo = new Field();
//        fieldTwo.setContainerUri(pair._2.getDescription());
//        fieldTwo.setFieldUri(pair._2.getUri());
//        topicComparison.setFieldTwo(fieldTwo);
//
//        Ranking<String> r1 = new Ranking<String>();
//        Ranking<String> r2 = new Ranking<String>();
//
//        for (int i = 0; i < maxWords; i++) {
//            r1.add(pair._1.getElements().get(i), pair._1.getScores().get(i));
//            r2.add(pair._2.getElements().get(i), pair._2.getScores().get(i));
//        }
//
//        Double score = new ExtendedKendallsTauDistance<String>().calculate(r1, r2, new
//                LevenshteinSimilarity());
//        topicComparison.setScore(score);
//        return topicComparison;
//    }

}
