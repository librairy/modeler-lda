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
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.boot.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class ShapesByTopicDao extends  AbstractDao{

    private static final Logger LOG = LoggerFactory.getLogger(ShapesByTopicDao.class);

    public static final String TOPIC = "topic";

    public static final String TYPE = "type";

    public static final String URI = "URI";

    public static final String VECTOR = "vector";

    public static final String TABLE = "shapes_by_topic";

    public ShapesByTopicDao() {
        super(TABLE);
    }

    public void initialize(String domainUri){
        createTable(TABLE, domainUri);
    }

    public void createTable(String table, String domainUri){
        try{
            getSession(domainUri).execute("create table if not exists "+table+"(" +
                    TOPIC +" bigint, " +
                    TYPE+" text, " +
                    URI +" text, " +
                    VECTOR+" list<double>, " +
                    "primary key ("+TOPIC+", "+TYPE+","+URI+"));");
            LOG.info("created LDA "+table+" table for domain: " + domainUri);
        }catch (InvalidQueryException e){
            LOG.warn(e.getMessage());
        }
    }


    public List<ShapeRow> get(String domainUri, Long topic, Optional<String> type, Optional<Integer> size, Optional<String> uri) throws DataNotFound {

        StringBuilder query = new StringBuilder().append("select uri, vector from " + table +" where topic="+ topic+"");

        if (type.isPresent()){
            query.append(" and type='").append(type.get()).append("'");
        }

        if (uri.isPresent()){
            query.append(" and token(topic,type,uri) > token(" +topic+",'"+ type.get()+"','" + uri.get() + "')");
        }

        query.append(" limit ").append((size.isPresent())? size.get().intValue() : 20);

        query.append(" ;");
        try{
            ResultSet result = getSession(domainUri).execute(query.toString());
            List<Row> rows = result.all();
            if ((rows == null) || (rows.isEmpty())) return Collections.emptyList();
            return rows.stream().map( row -> {
                ShapeRow shape = new ShapeRow();
                shape.setUri(row.getString(0));
                shape.setVector(row.getList(1, Double.class));
                return shape;
            }).collect(Collectors.toList());

        }catch (InvalidQueryException e){
            LOG.warn("Error on query execution: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    public Boolean save(String domainUri, ShapeRow row){

        List<Double> list = row.getVector();

        int topic = getMaxTopic(row.getVector());

        String type = URIGenerator.typeFrom(row.getUri()).key();

        String query = "insert into "+table+" (topic,type,uri,vector) values("+topic+", '"+type+"', '"+ row.getUri()+"', " + list +");";

        try{
            ResultSet result = getSession(domainUri).execute(query);
            LOG.info("saved shape_by_topic ["+topic+"/"+type+"/"+row.getUri()+"] in '"+domainUri+"'");

            return result.wasApplied();
        }catch (InvalidQueryException e){
            LOG.warn("Error on query execution: " + e.getMessage());
            return false;
        }
    }

    public int getMaxTopic(List<Double> topicDistribution){
        return IntStream.range(0,topicDistribution.size())
                .reduce((i,j) -> topicDistribution.get(i) > topicDistribution.get(j) ? i : j)
                .getAsInt();
    }

    @Override
    public void destroy(String domainUri){
        LOG.info("dropping existing LDA similarity  table for domain: " + domainUri);
        try{
            getSession(domainUri).execute("truncate "+table+";");
        }catch (InvalidQueryException e){
            LOG.warn(e.getMessage());
        }
    }

}
