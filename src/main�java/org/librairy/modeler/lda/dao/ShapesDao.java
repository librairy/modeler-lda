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
import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.boot.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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
    EventBus eventBus;

    public ShapesDao() {
        super(TABLE);
    }

    public void initialize(String domainUri){
        create(domainUri,TABLE);
        create(domainUri,CENTROIDS_TABLE);
    }

    public void create(String domainUri, String table){
        try{
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
        }catch (InvalidQueryException e){
            LOG.warn(e.getMessage());
        }
    }

    public boolean save(String domainUri, ShapeRow row){
        try{
            if (row.getUri() != null) row.setType(URIGenerator.typeFrom(row.getUri()).name());
        }catch (RuntimeException e){
            LOG.debug(e.getMessage());
        }
        return save(domainUri, row, TABLE);
    }

    public boolean saveCentroid(String domainUri, ShapeRow row){
        return save(domainUri, row, CENTROIDS_TABLE);
    }

    public boolean save(String domainUri, ShapeRow row, String table){

        String query = "insert into "+table+" ("+RESOURCE_ID+","+RESOURCE_URI+","+RESOURCE_TYPE+","+VECTOR+"," +
                ""+DATE+") " +
                "values ("+row.getId()+", '" + row.getUri() +"' , '"+ row.getType() + "', " + row.getVector() +", '" +TimeUtils
                .asISO() +"');";

        try{
            ResultSet result = getSession(domainUri).execute(query);
            LOG.info("saved shape: '"+row.getUri()+"' from '"+domainUri+"' in " + table + " table");

            // publish event
            Relation relation = new Relation();
            relation.setStartUri(row.getUri());
            relation.setEndUri(domainUri);
            eventBus.post(Event.from(relation), RoutingKey.of("shape.created"));

            return result.wasApplied();
        }catch (InvalidQueryException e){
            LOG.warn("Error on query execution: " + e.getMessage());
            return false;
        }
    }

    @Override
    public void destroy(String domainUri){
        LOG.info("dropping existing LDA shapes table for domain: " + domainUri);
        try{
            getSession(domainUri).execute("truncate "+table+";");
            getSession(domainUri).execute("truncate "+CENTROIDS_TABLE+";");
        }catch (InvalidQueryException e){
            LOG.warn(e.getMessage());
        }
    }

    public void destroyCentroids(String domainUri){
        LOG.info("dropping existing centroids shapes table for domain: " + domainUri);
        try{
            getSession(domainUri).execute("truncate "+CENTROIDS_TABLE+";");
        }catch (InvalidQueryException e){
            LOG.warn(e.getMessage());
        }
    }

    public List<ShapeRow> get(String domainUri, Optional<Integer> size, Optional<Long> id) throws DataNotFound {

        StringBuilder query = new StringBuilder().append("select id, uri, vector from " + table);

        if (id.isPresent()){
            query.append(" where token(id) > token(" + id.get() + ")");
        }

        query.append(" limit ").append((size.isPresent())? size.get().intValue() : 20);

        query.append(" ;");
        try{
            ResultSet result = getSession(domainUri).execute(query.toString());
            List<Row> rows = result.all();
            if ((rows == null) || (rows.isEmpty())) return Collections.emptyList();
            return rows.stream().map( row -> {
                ShapeRow shape = new ShapeRow();
                shape.setId(row.getLong(0));
                shape.setUri(row.getString(1));
                shape.setVector(row.getList(2, Double.class));
                return shape;
            }).collect(Collectors.toList());

        }catch (InvalidQueryException e){
            LOG.warn("Error on query execution: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    public ShapeRow get(String domainUri, String uri) throws DataNotFound {

        StringBuilder query = new StringBuilder().append("select id, uri, vector from " + table + " where uri='" + uri +"';");

        try{
            ResultSet result = getSession(domainUri).execute(query.toString());
            Row row = result.one();
            if ((row == null)) throw new RuntimeException("No found by uri: " + uri);
            ShapeRow shape = new ShapeRow();
            shape.setId(row.getLong(0));
            shape.setUri(row.getString(1));
            shape.setVector(row.getList(2, Double.class));
            return shape;

        }catch (InvalidQueryException e){
            LOG.warn("Error on query execution: " + e.getMessage());
            throw new RuntimeException("No found by uri: " + uri);
        }
    }

}
