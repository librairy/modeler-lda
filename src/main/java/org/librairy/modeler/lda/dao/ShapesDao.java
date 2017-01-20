/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.dao;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.modeler.lda.api.SessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class ShapesDao extends  AbstractDao{

    private static final Logger LOG = LoggerFactory.getLogger(ShapesDao.class);

    public static final String RESOURCE_URI = "uri";

    public static final String RESOURCE_ID = "id";

    public static final String RESOURCE_TYPE = "type";

    public static final String VECTOR = "vector";

    public static final String DATE = "date";

    public static final String TABLE = "shapes";

    public static final String CENTROIDS_TABLE = "shapecentroids";

    @Autowired
    SessionManager sessionManager;

    public ShapesDao() {
        super(TABLE);
    }

    public void initialize(String domainUri){
        create(domainUri,TABLE);
        create(domainUri,CENTROIDS_TABLE);
    }

    public void create(String domainUri, String table){
        ResultSet result = getSession(domainUri).execute("create table if not exists " +
                table + "(" +
                RESOURCE_ID + " bigint, " +
                RESOURCE_URI + " text, " +
                RESOURCE_TYPE + " text, " +
                VECTOR + " list<double>, " +
                DATE + " text, " +
                "primary key (" + RESOURCE_ID + "));");
        if (!result.wasApplied()){
            LOG.warn("Table " + table + " not created!!");
        }
        getSession(domainUri).execute("create index if not exists on "+table+" ("+RESOURCE_URI+");");
        getSession(domainUri).execute("create index if not exists on "+table+" ("+RESOURCE_TYPE+");");
        LOG.info("created LDA "+table+" table for domain: " + domainUri);
    }

    public boolean save(String domainUri, ShapeRow row){
        return save(domainUri, row, TABLE);
    }

    public boolean saveCentroid(String domainUri, ShapeRow row){
        return save(domainUri, row, CENTROIDS_TABLE);
    }

    public boolean save(String domainUri, ShapeRow row, String table){

        String type = "";
        try{
            type = URIGenerator.typeFrom(row.getUri()).name();
        }catch (RuntimeException e){
            LOG.debug(e.getMessage());
        }

        String query = "insert into "+table+" ("+RESOURCE_ID+","+RESOURCE_URI+","+RESOURCE_TYPE+","+VECTOR+"," +
                ""+DATE+") " +
                "values ("+row.getId()+", '" + row.getUri() +"' , '"+ type + "', " + row.getVector() +", '" +TimeUtils
                .asISO() +"');";

        try{
            ResultSet result = sessionManager.getSession(domainUri).execute(query);
            LOG.info("saved shape: '"+row.getUri()+"' from '"+domainUri+"' in " + table + " table");
            return result.wasApplied();
        }catch (InvalidQueryException e){
            LOG.warn("Error on query execution: " + e.getMessage());
            return false;
        }
    }

    @Override
    public void destroy(String domainUri){
        LOG.info("dropping existing LDA shapes table for domain: " + domainUri);
        sessionManager.getSession(domainUri).execute("truncate "+table+";");
        sessionManager.getSession(domainUri).execute("truncate "+CENTROIDS_TABLE+";");
    }

}
