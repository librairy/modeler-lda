/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.optimizer;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.metrics.data.Pair;
import org.librairy.metrics.data.Ranking;
import org.librairy.metrics.distance.ExtendedKendallsTauDistance;
import org.librairy.metrics.utils.Permutations;
import org.librairy.modeler.lda.Application;
import org.librairy.modeler.lda.api.LDAModelerAPI;
import org.librairy.modeler.lda.api.model.Criteria;
import org.librairy.modeler.lda.api.model.ScoredTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@TestPropertySource(properties = {
        "librairy.columndb.host = zavijava.dia.fi.upm.es",
        "librairy.documentdb.host = zavijava.dia.fi.upm.es",
        "librairy.graphdb.host = zavijava.dia.fi.upm.es",
        "librairy.eventbus.host = zavijava.dia.fi.upm.es"
//        "librairy.uri = drinventor.eu" //librairy.org
})
public class TopicOptimizerTest {

    private static final Logger LOG = LoggerFactory.getLogger(TopicOptimizerTest.class);

    @Autowired
    LDAModelerAPI api;

    @Test
    public void distanceBetweenTopics(){

        Criteria criteria = new Criteria();
        criteria.setMax(100);

        List<ScoredTopic> topics = api.getTopics(criteria);


        LOG.info("creating rankings from topics...");
        List<Ranking<String>> rankings = topics.stream().map(st -> {
            Ranking<String> ranking = new Ranking<String>();
            st.getWords().forEach(sw -> ranking.add(sw.getWord(), sw.getScore()));
            ranking.setId(String.valueOf(st.getId()));
            return ranking;
        }).collect(Collectors.toList());


        LOG.info("pairing rankings..");
        Permutations<Ranking<String>> permutations = new Permutations<>();
        Set<Pair<Ranking<String>>> pairs = permutations.sorted(rankings);

        LOG.info("calculating distances..");
        ExtendedKendallsTauDistance<String> measure = new ExtendedKendallsTauDistance<>();
        pairs.forEach( pair -> {
            Double distance = measure.calculate(pair.getI(), pair.getJ(), (w1, w2) -> 0.5);
            LOG.info("Distance ["+pair.getI().getId() + " - " + pair.getJ().getId() + " ] = " + distance);
        });

//        pairs
//                .stream()
//                .map( pair -> new PairDistance(pair.getI().getId(), pair.getJ().getId(),measure.calculate(pair.getI(), pair.getJ(), (w1, w2) -> 0.5) ))
//                .sorted((p1,p2) -> p1.get)



    }
}
