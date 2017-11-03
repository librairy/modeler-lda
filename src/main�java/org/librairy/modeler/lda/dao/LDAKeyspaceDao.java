/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.dao;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.librairy.boot.storage.dao.DBSessionManager;
import org.librairy.boot.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class LDAKeyspaceDao {

    private static final Logger LOG = LoggerFactory.getLogger(LDAKeyspaceDao.class);

    @Autowired
    DBSessionManager sessionManager;

    public void initialize(String domainUri){
        LOG.info("creating a new LDA workspace for domain: " + domainUri);
        try{
            sessionManager.getSession().execute("create keyspace if not exists "+DBSessionManager.getSpecificKeyspaceId("lda", URIGenerator.retrieveId(domainUri))+
                    " with replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1} and DURABLE_WRITES = true;");
        }catch (InvalidQueryException e){
            LOG.warn(e.getMessage());
        }
    }

    public void destroy(String domainUri){
        LOG.info("dropping existing LDA workspace for domain: " + domainUri);
        try{
            sessionManager.getSession().execute("drop keyspace if exists " + DBSessionManager.getSpecificKeyspaceId("lda", URIGenerator.retrieveId(domainUri))+ ";");
        }catch (InvalidQueryException e){
            LOG.warn(e.getMessage());
        }
    }

}
