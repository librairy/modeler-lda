/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.api;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Doubles;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.librairy.boot.model.domain.resources.Domain;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.modeler.lda.api.model.Criteria;
import org.librairy.modeler.lda.api.model.ScoredResource;
import org.librairy.modeler.lda.api.model.ScoredTopic;
import org.librairy.modeler.lda.api.model.ScoredWord;
import org.librairy.modeler.lda.dao.*;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.*;
import org.librairy.modeler.lda.services.ShortestPathService;
import org.librairy.modeler.lda.tasks.LDAComparisonTask;
import org.librairy.modeler.lda.tasks.LDATextTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class LDAModelerAPI {

    private static final Logger LOG = LoggerFactory.getLogger(LDAModelerAPI.class);

    @Autowired
    SessionManager sessionManager;

    @Autowired
    ModelingHelper helper;

    @Autowired
    ShortestPathAPI shortestPathAPI;

    @Autowired
    FreeTextAPI freeTextAPI;


    public String getItemFromDocument(String uri) throws DataNotFound {
        // TODO handle criteria.type
        String query = "select enduri from research.bundles where starturi='"+uri+"';";


        LOG.debug("Executing query: " + query);
        try{
            ResultSet result = sessionManager.getSession().execute(query);
            Row row = result.one();

            if (row == null ) return "";

            return row.getString(0);
        }catch (InvalidQueryException e){
            LOG.warn("Query error: " + e.getMessage());
            throw new DataNotFound("error executing query");
        }catch (Exception e){
            LOG.error("Unexpected error", e);
            throw new DataNotFound("error executing query");
        }

    }


    public List<ScoredResource> getMostRelevantResources(String topicUri, Criteria criteria){

        // TODO handle criteria.type
        String query = "select "+ DistributionsDao.RESOURCE_URI + "," + DistributionsDao.RESOURCE_TYPE + ","+ DistributionsDao.SCORE + "," + DistributionsDao.DATE
                + " from " + DistributionsDao.TABLE
                + " where " + DistributionsDao.TOPIC_URI+"='"+topicUri+"'"
                + " order by "+ DistributionsDao.SCORE + " DESC"
                + " limit " + criteria.getMax()+";";


        LOG.debug("Executing query: " + query);
        try{
            ResultSet result = sessionManager.getSession(criteria.getDomainUri()).execute(query);
            List<Row> rows = result.all();

            if (rows == null || rows.isEmpty()) return Collections.emptyList();

            return rows
                    .stream()
                    .map(row -> new ScoredResource(row.getString(0), row.getString(1), row.getDouble(2), row.getString(3)))
                    .collect(Collectors.toList());
        }catch (InvalidQueryException e){
            LOG.warn("Query error: " + e.getMessage());
            return Collections.emptyList();
        }catch (Exception e){
            LOG.error("Unexpected error", e);
            return Collections.emptyList();
        }
    }


    public List<ScoredTopic> getTopicsDistribution(String resourceUri, Criteria criteria, Integer maxWords){

        String query = "select "+ ShapesDao.VECTOR
                + " from " + ShapesDao.TABLE
                + " where " + ShapesDao.RESOURCE_URI+"='"+resourceUri+"' ALLOW FILTERING;";


        LOG.debug("Executing query: " + query);
        try{
            ResultSet result = sessionManager.getSession(criteria.getDomainUri()).execute(query);
            Row row = result.one();

            if (row == null ) return Collections.emptyList();

            List<Double> scores = row.getList(0, Double.class);

            List<ScoredTopic> topics = new ArrayList<>();

            for (int i=0; i < scores.size(); i++){
                ScoredTopic scoredTopic = null;
                try {
                    scoredTopic = getTopicDistribution(criteria.getDomainUri(), Long.valueOf(i), maxWords);
                    scoredTopic.setRelevance(scores.get(i));
                    scoredTopic.setDescription(String.valueOf(i));
                    topics.add(scoredTopic);
                } catch (DataNotFound dataNotFound) {
                    continue;
                }
            }

            return topics.stream().sorted((t1,t2)->-t1.getRelevance().compareTo(t2.getRelevance())).limit(criteria
                    .getMax()).collect(Collectors.toList());
        }catch (InvalidQueryException e){
            LOG.warn("Query error: " + e.getMessage());
            return Collections.emptyList();
        }catch (Exception e){
            LOG.error("Unexpected error", e);
            return Collections.emptyList();
        }

    }

    public List<ScoredTopic> getTopics(Criteria criteria){

        String query = "select "+ TopicsDao.ID
                + " from " + TopicsDao.TABLE
                +";";


        LOG.debug("Executing query: " + query);
        try{
            ResultSet result = sessionManager.getSession(criteria.getDomainUri()).execute(query);
            List<Row> rows = result.all();

            if (rows == null ) return Collections.emptyList();


            return rows.stream().map(row -> {
                try {
                    return getTopicDistribution(criteria.getDomainUri(), row.getLong(0), criteria.getMax());
                } catch (DataNotFound dataNotFound) {
                    return null;
                }
            })
                    .collect
                    (Collectors.toList());
        }catch (InvalidQueryException e){
            LOG.warn("Query error: " + e.getMessage());
            return Collections.emptyList();
        }catch (Exception e){
            LOG.error("Unexpected error", e);
            return Collections.emptyList();
        }

    }


    private ScoredTopic getTopicDistribution(String domainUri, Long topicId, Integer maxSize) throws DataNotFound {

        ScoredTopic topic = new ScoredTopic();

        String query = "select "+ TopicsDao.URI+ "," + TopicsDao.ELEMENTS+ ","+ TopicsDao.SCORES
                + " from " + TopicsDao.TABLE
                + " where " + TopicsDao.ID+"="+topicId+" ALLOW FILTERING;";


        LOG.debug("Executing query: " + query);
        try{
            ResultSet result = sessionManager.getSession(domainUri).execute(query);

            Row row = result.one();

            if (row == null) throw new DataNotFound("No topic found in domain '" + domainUri + "'");

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
        }catch (InvalidQueryException e){
            LOG.warn("Query error: " + e.getMessage());
            throw new DataNotFound("Error on query");
        }catch (Exception e){
            LOG.error("Unexpected error", e);
            throw new DataNotFound("Error on query");
        }

    }

    public List<ScoredWord> getTags(String resourceUri, Criteria criteria){

        // todo avoid ALLOW FILTERING
        // todo handle type='tags' and criteria.type
        String query = "select "+ AnnotationsDao.VALUE + "," + AnnotationsDao.SCORE
                + " from " + AnnotationsDao.TABLE
                + " where " + AnnotationsDao.RESOURCE_URI+"='"+resourceUri+"'"
                + " ALLOW FILTERING;";


        LOG.debug("Executing query: " + query);
        try{
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
                    .limit(criteria.getMax())
                    .collect(Collectors.toList())
            ;
        }catch (InvalidQueryException e){
            LOG.warn("Query error: " + e.getMessage());
            return Collections.emptyList();
        }catch (Exception e){
            LOG.error("Unexpected error", e);
            return Collections.emptyList();
        }


    }

    public List<ScoredResource> getSimilarResources(String resourceUri, Criteria criteria){

        StringBuilder queryBuilder = new StringBuilder().append("select ")
                .append(SimilaritiesDao.RESOURCE_URI_2).append(", ").append(SimilaritiesDao.SCORE).append(", ").append(SimilaritiesDao.DATE)
                .append(" from ").append(SimilaritiesDao.TABLE)
                .append(" where ").append(SimilaritiesDao.RESOURCE_URI_1).append("='").append(resourceUri).append("' ");

        if (!criteria.getTypes().isEmpty()){
            // TODO handle type filter
            queryBuilder = queryBuilder.append(" and ").append(SimilaritiesDao.RESOURCE_TYPE_2).append("='").append
                    (criteria.getTypes().get(0).key()).append("' ");
        }

        queryBuilder = queryBuilder.append(" and score >= ").append(criteria.getThreshold())
//                .append(" order by ").append(SimilaritiesDao.SCORE).append(" desc")
                .append(" limit ").append(criteria.getMax()).append(";");

        String query = queryBuilder.toString();

        LOG.debug("Executing query: " + query);
        try{
            ResultSet result = sessionManager.getSession(criteria.getDomainUri()).execute(query);
            List<Row> rows = result.all();

            if (rows == null || rows.isEmpty()) return Collections.emptyList();

            return rows
                    .stream()
                    .map(row -> new ScoredResource(row.getString(0), URIGenerator.typeFrom(row.getString(0)).key(),row.getDouble(1), row.getString(2)))
                    .collect(Collectors.toList());
        }catch (InvalidQueryException e){
            LOG.warn("Query error: " + e.getMessage());
            return Collections.emptyList();
        }catch (Exception e){
            LOG.error("Unexpected error", e);
            return Collections.emptyList();
        }


    }

    public List<ScoredResource> getSimilarResources(Text text, Criteria criteria){

        LOG.info("Getting similar resources to a given text by criteria: " + criteria);
        try{
            List<String> types = criteria.getTypes().stream().map(type -> type.key()).collect(Collectors.toList());

            return freeTextAPI
                    .getSimilarResourcesTo(text, criteria.getDomainUri(), criteria.getThreshold(), criteria.getMax(), types)
                    .stream()
                    .map(simRes -> new ScoredResource(simRes.getUri(), "", simRes.getWeight(), simRes.getTime()))
                    .collect(Collectors.toList());

        }catch (Exception e){
            LOG.error("Unexpected error", e);
            return Collections.emptyList();
        }
    }

    public List<Comparison<Field>> compareTopicsFrom(List<String> domains, Criteria criteria){

        LOG.info("Comparing domains: " + domains + " by criteria: " + criteria);
        if ((domains == null) || (domains.size() < 2)) return Collections.emptyList();

        try {
            // sort domains
            List<Domain> sortedDomains = domains.stream().map(uri -> {
                try {
                    Domain domain = new Domain();
                    domain.setUri(uri);
                    domain.setCreationTime(helper.getComparisonsDao().getDate(domain.getUri()));
                    return domain;
                } catch (DataNotFound dataNotFound) {
                    return null;
                }
            }).filter(r -> r != null)
                    .sorted((a, b) -> -a.getCreationTime().compareTo(b.getCreationTime())).collect
                            (Collectors.toList());

            if (sortedDomains.size() < 2) return Collections.emptyList();

            // compose comparisons
            return compare(sortedDomains.get(0), sortedDomains.subList(1, sortedDomains.size()), criteria).collect(Collectors.toList());
        }catch (Exception e){
            LOG.error("Unexpected error", e);
            return Collections.emptyList();
        }
    }


    private Stream<Comparison<Field>> compare(Domain domain, List<Domain> domains, Criteria criteria){

        if (domains.size() == 1) return compare(domain, domains.get(0), criteria);

        return Stream.concat(domains.stream().flatMap(d -> compare(domain, d, criteria)), compare(domains.get(0), domains
                .subList(1,domains.size()), criteria));

    }


    private Stream<Comparison<Field>> compare(Domain domain1, Domain domain2, Criteria criteria){
        return helper.getComparisonsDao().get(domain1.getUri(), domain2.getUri()).stream()
                .filter(c -> c.getScore()> criteria.getThreshold())
                .sorted((a,b) -> -a.getScore().compareTo(b.getScore()))
                .limit(criteria.getMax())
                .map( res
                -> {
            Comparison<Field> comparison = new Comparison<Field>();
            Field f1 = new Field();
            f1.setContainerUri(domain1.getUri());
            f1.setFieldUri(res.getTopic_uri());
            comparison.setFieldOne(f1);
            Field f2 = new Field();
            f2.setContainerUri(res.getDomain_uri());
            f2.setFieldUri(res.getDomain_topic_uri());
            comparison.setFieldTwo(f2);
            comparison.setScore(res.getScore());
            return comparison;
        });
    }



    public List<Path> getShortestPath(String startUri, String endUri, List<String> types, Integer maxLength, Criteria criteria, Integer maxMinutes) throws IllegalArgumentException, DataNotFound, InterruptedException {

        return shortestPathAPI.calculate(startUri, endUri, criteria.getDomainUri(), criteria.getThreshold(), maxLength, criteria.getMax(), types);

    }


    public Double compare(String uri1,String uri2, String domainUri){

        List<double[]> vectors = Arrays.asList(new String[]{uri1, uri2}).stream().map(uri -> {
            String query = "select vector from shapes where uri='" + uri + "';";
            ResultSet result = sessionManager.getSession(domainUri).execute(query);
            List<Double> vector = result.one().getList(0, Double.class);
            return Doubles.toArray(vector);

        }).collect(Collectors.toList());

        double score = JensenShannonSimilarity.apply(vectors.get(0), vectors.get(1));

        return score;

    }


}
