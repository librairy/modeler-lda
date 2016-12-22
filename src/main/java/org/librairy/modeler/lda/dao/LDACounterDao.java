/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.dao;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
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
public class LDACounterDao {

    private static final Logger LOG = LoggerFactory.getLogger(LDACounterDao.class);

    @Autowired
    SessionManager sessionManager;


    public Boolean initialize(String domainUri){

        String query = "create table if not exists counts(num counter, topic bigint, name varchar, primary key" +
                "(topic, name));";

        ResultSet result = sessionManager.getSession(domainUri).execute(query);

        return result.wasApplied();
    }

    public Boolean remove(String domainUri){

        String query = "drop table if exists counts;";

        ResultSet result = sessionManager.getSession(domainUri).execute(query);

        return result.wasApplied();
    }

    public Boolean increment(String domainUri, Long topicId, String counter, Long value){

        String query = "update counts set num = num + "+value+" where topic="+topicId+" and name='"+counter+"';";

        ResultSet result = sessionManager.getSession(domainUri).execute(query);

        return result.wasApplied();
    }

    public Long get(String domainUri, Long topicId, String counter){

        String query = "select num from counts where topic="+topicId+" and name='"+counter+"';";

        ResultSet result = sessionManager.getSession(domainUri).execute(query);

        Row row = result.one();

        if (row == null) return 0l;

        return row.getLong(0);

    }

}