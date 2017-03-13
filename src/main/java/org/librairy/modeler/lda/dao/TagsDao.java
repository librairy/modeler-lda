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
public class TagsDao extends AbstractDao{

    public static final Logger LOG = LoggerFactory.getLogger(TagsDao.class);

    public static final String WORD = "word";

    public static final String SCORE = "score";

    public static final String TABLE = "tags";

    public TagsDao() {
        super(TABLE);
    }

    public void initialize(String domainUri){
        LOG.info("creating a LDA tags table for domain: " + domainUri);
        try{
            getSession(domainUri).execute("create table if not exists "+table+"(" +
                    WORD+" text, " +
                    SCORE+" double, " +
                    "primary key ("+WORD+"));");
        }catch (InvalidQueryException e){
            LOG.warn(e.getMessage());
        }
    }


    public List<TagRow> get(String domainUri, Integer maxWords){
        String query = "select word, score from "+TABLE+";";

        try{
            ResultSet result = getSession(domainUri).execute(query);
            List<Row> rows = result.all();

            if ((rows == null) || rows.isEmpty()) return Collections.emptyList();

            return rows.stream().map(row -> {
                TagRow tagRow = new TagRow();
                tagRow.setWord(row.getString(0));
                tagRow.setScore(row.getDouble(1));
                return tagRow;

            })
                    .sorted( (a,b) -> -a.getScore().compareTo(b.getScore()))  //TODO handle sort directly in cassandra
                    .limit(maxWords)
                    .collect(Collectors.toList());
        } catch (InvalidQueryException e){
            LOG.warn("Error on query: " + e.getMessage());
            return Collections.emptyList();
        }
    }




}
