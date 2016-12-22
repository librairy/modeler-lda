/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.dao;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.librairy.metrics.data.Ranking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class TopicsDao extends AbstractDao{

    public static final Logger LOG = LoggerFactory.getLogger(TopicsDao.class);

    public static final String URI = "uri";

    public static final String ID = "id";

    public static final String DESCRIPTION = "description";

    public static final String ELEMENTS = "elements";

    public static final String SCORES = "scores";

    public static final String DATE = "date";

    public static final String TABLE = "topics";

    public TopicsDao() {
        super(TABLE);
    }

    public void initialize(String domainUri){
        LOG.info("creating a LDA topics table for domain: " + domainUri);
        getSession(domainUri).execute("create table if not exists "+table+"(" +
                URI+" text, " +
                ID+" bigint, " +
                DESCRIPTION+" text, " +
                ELEMENTS+" list<text>, " +
                SCORES+" list<double>, " +
                DATE+" text, " +
                "primary key ("+URI+","+ID+"));");
    }


    public List<TopicRow> list(String domainUri, Integer maxWords){
        String query = "select uri, id, date, description, elements, scores from "+TABLE+";";

        try{
            ResultSet result = getSession(domainUri).execute(query);
            List<Row> rows = result.all();

            if ((rows == null) || rows.isEmpty()) return Collections.emptyList();

            return rows.stream().map(row -> {
                TopicRow topicRow = new TopicRow();
                topicRow.setUri(row.getString(0));
                topicRow.setId(row.getLong(1));
                topicRow.setDate(row.getString(2));
                topicRow.setDescription(row.getString(3));

                List<String> words = row.getList(4, String.class).subList(0,maxWords);
                topicRow.setElements(words);

                List<Double> scores = row.getList(5, Double.class).subList(0,maxWords);
                topicRow.setScores(scores);
                return topicRow;

            }).collect(Collectors.toList());
        } catch (InvalidQueryException e){
            LOG.warn("Error on query: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    public List<TopicRank> listAsRank(String domainUri, Integer maxWords){
        String query = "select uri, elements, scores from "+TABLE+";";

        try{
            ResultSet result = getSession(domainUri).execute(query);
            List<Row> rows = result.all();

            if ((rows == null) || rows.isEmpty()) return Collections.emptyList();

            return rows.stream().map(row -> {
                TopicRank topicRank = new TopicRank();
                topicRank.setDomainUri(domainUri);
                topicRank.setTopicUri(row.getString(0));

                List<String> words = row.getList(1, String.class);
                List<Double> scores = row.getList(2, Double.class);
                Ranking<String> ranking = new Ranking<>();
                for (int i=0; i < maxWords; i++){
                    ranking.add(words.get(i),scores.get(i));
                }
                topicRank.setWords(ranking);

                return topicRank;

            }).collect(Collectors.toList());
        } catch (InvalidQueryException e){
            LOG.warn("Error on query: " + e.getMessage());
            return Collections.emptyList();
        }
    }




}
