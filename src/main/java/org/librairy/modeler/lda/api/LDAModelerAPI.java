/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.api;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.librairy.modeler.lda.api.model.Criteria;
import org.librairy.modeler.lda.api.model.ScoredResource;
import org.librairy.modeler.lda.api.model.ScoredWord;
import org.librairy.modeler.lda.api.model.ScoredTopic;
import org.librairy.modeler.lda.dao.AnnotationsDao;
import org.librairy.modeler.lda.dao.DistributionsDao;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.dao.TopicsDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class LDAModelerAPI {

    private static final Logger LOG = LoggerFactory.getLogger(LDAModelerAPI.class);

    @Autowired
    SessionManager sessionManager;


    public List<ScoredResource> getMostRelevantResources(String topicUri, Criteria criteria){

        // TODO handle criteria.type
        String query = "select "+ DistributionsDao.RESOURCE_URI + "," + DistributionsDao.RESOURCE_TYPE + ","+ DistributionsDao.SCORE
                + " from " + DistributionsDao.TABLE
                + " where " + DistributionsDao.TOPIC_URI+"='"+topicUri+"'"
                + " order by "+ DistributionsDao.SCORE + " DESC"
                + " limit " + criteria.getMax()+";";


        LOG.info("Executing query: " + query);
        ResultSet result = sessionManager.getSession(criteria.getDomainUri()).execute(query);

        List<Row> rows = result.all();

        if (rows == null || rows.isEmpty()) return Collections.emptyList();

        return rows
                .stream()
                .map(row -> new ScoredResource(row.getString(0), row.getString(1), row.getDouble(2)))
                .collect(Collectors.toList());
    }


    public List<ScoredTopic> getTopicsDistribution(String resourceUri, Criteria criteria){

        String query = "select "+ ShapesDao.VECTOR
                + " from " + ShapesDao.TABLE
                + " where " + ShapesDao.RESOURCE_URI+"='"+resourceUri+"' ALLOW FILTERING;";


        LOG.info("Executing query: " + query);
        ResultSet result = sessionManager.getSession(criteria.getDomainUri()).execute(query);

        Row row = result.one();

        if (row == null ) return Collections.emptyList();

        List<Double> scores = row.getList(0, Double.class);

        List<ScoredTopic> topics = new ArrayList<>();

        for (int i=0; i < scores.size(); i++){
            ScoredTopic scoredTopic = getTopicDistribution(criteria.getDomainUri(), Long.valueOf(i), criteria.getMax());
            scoredTopic.setRelevance(scores.get(i));
            scoredTopic.setDescription(String.valueOf(i));
            topics.add(scoredTopic);
        }

        return topics.stream().sorted((t1,t2)->-t1.getRelevance().compareTo(t2.getRelevance())).collect(Collectors.toList());
    }

    public List<ScoredTopic> getTopics(Criteria criteria){

        String query = "select "+ TopicsDao.ID
                + " from " + TopicsDao.TABLE+";";


        LOG.info("Executing query: " + query);
        ResultSet result = sessionManager.getSession(criteria.getDomainUri()).execute(query);

        List<Row> rows = result.all();

        if (rows == null ) return Collections.emptyList();


        return rows.stream().map(row -> getTopicDistribution(criteria.getDomainUri(), row.getLong(0),criteria.getMax()))
                .collect
                (Collectors.toList());
    }


    private ScoredTopic getTopicDistribution(String domainUri, Long topicId, Integer maxSize){

        ScoredTopic topic = new ScoredTopic();

        String query = "select "+ TopicsDao.URI+ "," + TopicsDao.ELEMENTS+ ","+ TopicsDao.SCORES
                + " from " + TopicsDao.TABLE
                + " where " + TopicsDao.ID+"="+topicId+" ALLOW FILTERING;";


        LOG.info("Executing query: " + query);
        ResultSet result = sessionManager.getSession(domainUri).execute(query);

        Row row = result.one();

        if (row == null) return topic;

        String topicUri     = row.getString(0);
        List<String> words  = row.getList(1, String.class);
        List<Double> scores = row.getList(2, Double.class);

        topic.setUri(topicUri);
        topic.setId(topicId);
        List<ScoredWord> scoredWords = new ArrayList<>();
        for (int i=0; i< maxSize; i++){
            scoredWords.add(new ScoredWord(words.get(i),scores.get(i)));
        }
        topic.setWords(scoredWords);
        return topic;

    }

    public List<ScoredWord> getTags(String resourceUri, Criteria criteria){

        // todo handle type='tags' and criteria.type
        String query = "select "+ AnnotationsDao.VALUE + "," + AnnotationsDao.SCORE
                + " from " + AnnotationsDao.TABLE
                + " where " + AnnotationsDao.RESOURCE_URI+"='"+resourceUri+"'"
                + " ALLOW FILTERING;";


        LOG.info("Executing query: " + query);
        ResultSet result = sessionManager.getSession(criteria.getDomainUri()).execute(query);

        List<Row> rows = result.all();

        if (rows == null || rows.isEmpty()) return Collections.emptyList();

        return rows
                .stream()
                .filter(row -> row.getDouble(1)>0.0)
                .collect(Collectors.groupingBy(t -> t.getString(0), Collectors.averagingDouble(r -> r.getDouble(1))))
                .entrySet()
                .stream()
                .map(entry -> new ScoredWord(entry.getKey(),entry.getValue()))
                .sorted((a,b)->-a.getScore().compareTo(b.getScore()))
                .collect(Collectors.toList())
        ;

    }

    public List<ScoredResource> getSimilarResources(String resourceUri, Criteria criteria){
        return null;
    }

    public List<ScoredResource> getDiscoveryPath(String startUri, String endUri, Criteria criteria){
        return null;
    }


}
