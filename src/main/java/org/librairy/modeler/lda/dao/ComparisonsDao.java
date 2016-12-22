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
import org.librairy.boot.storage.exception.DataNotFound;
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
public class ComparisonsDao extends  AbstractDao{

    private static final Logger LOG = LoggerFactory.getLogger(ComparisonsDao.class);

    public static final String DOMAIN_URI = "domain_uri";

    public static final String DOMAIN_TOPIC_URI = "domain_topic_uri";

    public static final String TOPIC_URI = "topic_uri";

    public static final String SCORE = "score";

    public static final String DATE = "date";

    public static final String TABLE = "comparisons";

    public ComparisonsDao() {
        super(TABLE);
    }

    public void initialize(String domainUri){
        LOG.info("creating LDA comparisons table for domain: " + domainUri);

        getSession(domainUri).execute("create table if not exists "+table+"(" +
                DOMAIN_URI +" text, " +
                DOMAIN_TOPIC_URI +" text, " +
                TOPIC_URI+" text, " +
                SCORE+" double, " +
                DATE+" text, " +
                "primary key ("+DOMAIN_URI+", "+TOPIC_URI+","+SCORE+","+ DOMAIN_TOPIC_URI+"))" +
                "with clustering order by ("+TOPIC_URI+" ASC, "+SCORE+" DESC, "+ DOMAIN_TOPIC_URI +" ASC );");
    }

    public List<ComparisonRow> get(String domainUri1, String domainUri2){

        String query ="select topic_uri, score, domain_topic_uri, date " +
                "from " + TABLE +
                " where domain_uri='" + domainUri2+"';";

        try{
            ResultSet resultSet = getSession(domainUri1).execute(query);

            List<Row> rows = resultSet.all();

            if ((rows == null) || (rows.isEmpty())) return Collections.emptyList();

            return rows.stream().map(row -> new ComparisonRow(domainUri2, row.getString(2), row.getString(0), row.getString(3), row.getDouble(1))).collect(Collectors.toList());

        }catch (InvalidQueryException e){
            LOG.warn("Error on query: [" + query + "] : " + e.getMessage());
            return Collections.emptyList();
        }
    }

    public String getDate(String domainUri) throws DataNotFound {
        String query ="select date " +
                "from " + TABLE +
                " limit 1;";

        try{
            ResultSet resultSet = getSession(domainUri).execute(query);

            Row row = resultSet.one();

            if ((row == null)) throw new DataNotFound("No comparisons calculated for '" + domainUri + "'");

            return row.getString(0);

        }catch (InvalidQueryException e){
            LOG.warn("Error on query: [" + query + "] : " + e.getMessage());
            throw new DataNotFound("No comparisons calculated for '" + domainUri + "'");
        }
    }


}
