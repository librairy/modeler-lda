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
import org.librairy.modeler.lda.models.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class SimilaritiesDao extends  AbstractDao{

    private static final Logger LOG = LoggerFactory.getLogger(SimilaritiesDao.class);

    public static final String RESOURCE_URI_1 = "resource_uri_1";

    public static final String RESOURCE_TYPE_1 = "resource_type_1";

    public static final String RESOURCE_URI_2 = "resource_uri_2";

    public static final String RESOURCE_TYPE_2 = "resource_type_2";

    public static final String SCORE = "score";

    public static final String DATE = "date";

    public static final String TABLE = "similarities";

    public static final String CENTROIDS_TABLE = "simcentroids";

    public SimilaritiesDao() {
        super(TABLE);
    }

    public void initialize(String domainUri){
        createTable(TABLE, domainUri);
        createTable(CENTROIDS_TABLE, domainUri);
    }

    public void createTable(String table, String domainUri){
        try{
            getSession(domainUri).execute("create table if not exists "+table+"(" +
                    RESOURCE_URI_1 +" text, " +
                    RESOURCE_TYPE_1+" text, " +
                    RESOURCE_URI_2 +" text, " +
                    RESOURCE_TYPE_2+" text, " +
                    SCORE+" double, " +
                    DATE+" text, " +
                    "primary key ("+RESOURCE_URI_1+", "+SCORE+","+RESOURCE_URI_2+"))" +
                    "with clustering order by ("+SCORE+" DESC, "+RESOURCE_URI_2+" ASC"+ ");");
            getSession(domainUri).execute("create index if not exists on "+table+" ("+RESOURCE_URI_2+");");
            getSession(domainUri).execute("create index if not exists on "+table+" ("+RESOURCE_TYPE_2+");");
            LOG.info("created LDA "+table+" table for domain: " + domainUri);
        }catch (InvalidQueryException e){
            LOG.warn(e.getMessage());
        }
    }


    public Double getSimilarity(String domainUri, String uri1, String uri2) throws DataNotFound {
        return getScore(domainUri, uri1, uri2, TABLE);
    }

    public Double getCentroidSimilarity(String domainUri, String uri1, String uri2) throws DataNotFound {
        return getScore(domainUri, uri1, uri2, CENTROIDS_TABLE);
    }

    public Double getScore(String domainUri, String uri1, String uri2, String table) throws DataNotFound {
        String query = "select "+SCORE+" from "+table+" where "+RESOURCE_URI_1+"='"+uri1+"' and " +RESOURCE_URI_2+"='"+uri2+"';";

        try{
            ResultSet result = getSession(domainUri).execute(query);
            Row row = result.one();
            if (row == null) throw new DataNotFound("No similarity found between '" +uri1+"' and '"+uri2+"'");
            return row.getDouble(0);

        }catch (InvalidQueryException e){
            LOG.warn("Error on query execution: " + e.getMessage());
            return 0.0;
        }
    }

    @Override
    public void destroy(String domainUri){
        LOG.info("dropping existing LDA similarity  table for domain: " + domainUri);
        try{
            sessionManager.getSession(domainUri).execute("truncate "+table+";");
            sessionManager.getSession(domainUri).execute("truncate "+CENTROIDS_TABLE+";");
        }catch (InvalidQueryException e){
            LOG.warn(e.getMessage());
        }
    }

}
