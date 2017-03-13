/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.dao;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.modeler.lda.api.SessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class ClusterDao extends  AbstractDao{

    private static final Logger LOG = LoggerFactory.getLogger(ClusterDao.class);

    public static final String URI = "uri";

    public static final String CLUSTER = "cluster";

    public static final String TABLE = "clusters";


    @Autowired
    SessionManager sessionManager;

    public ClusterDao() {
        super(TABLE);
    }

    public void initialize(String domainUri){
        create(domainUri,TABLE);
    }

    public void create(String domainUri, String table){
        try{
            ResultSet result = getSession(domainUri).execute("create table if not exists " +
                    table + "(" +
                    URI + " text, " +
                    CLUSTER+ " bigint, " +
                    "primary key ( " + URI+ " , " + CLUSTER + " ));");
            if (!result.wasApplied()){
                LOG.warn("Table " + table + " not created!!");
            }
            LOG.info("created LDA "+table+" table for domain: " + domainUri);
        }catch (InvalidQueryException e){
            LOG.warn(e.getMessage());
        }
    }

    public boolean save(String domainUri, ClusterRow row){
        return save(domainUri, row, TABLE);
    }

    public boolean save(String domainUri, ClusterRow row, String table){
        String query = "insert into "+table+" ("+URI+","+CLUSTER+ ") " +
                "values ( '"+row.getUri()+"' , " + row.getCluster() +");";

        try{
            ResultSet result = sessionManager.getSession(domainUri).execute(query);
            LOG.info("assigned : "+row.getUri()+"' in '"+domainUri+"' to cluster " + row.getCluster());
            return result.wasApplied();
        }catch (InvalidQueryException e){
            LOG.warn("Error on query execution: " + e.getMessage());
            return false;
        }
    }

    public List<Long> getClusters(String domainUri, String uri) throws DataNotFound {
        String query = "select "+CLUSTER+" from "+TABLE+" where "+URI+"='"+uri+ "';";

        try{
            ResultSet result = sessionManager.getSession(domainUri).execute(query);

            List<Row> rows = result.all();

            if (rows == null || rows.isEmpty()) throw new DataNotFound("No clusters found by uri='"+uri+"'");

            return rows.stream().map(row -> row.getLong(0)).collect(Collectors.toList());
        }catch (InvalidQueryException e){
            LOG.warn("Error on query execution: " + e.getMessage());
            return Collections.emptyList();
        }
    }


}
