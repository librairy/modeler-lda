/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.api;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.librairy.storage.generator.URIGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.config.CassandraClusterFactoryBean;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class SessionManager {

    @Autowired
    CassandraClusterFactoryBean clusterFactoryBean;

    ConcurrentHashMap<String,Session> sessions;

    private Cluster cluster;

    private Session controlSession;

    @PostConstruct
    public void setup(){
        this.cluster    = clusterFactoryBean.getObject();
        this.sessions   = new ConcurrentHashMap<>();
    }


    public Session getSession(String domainUri){
        Session session = sessions.get(domainUri);

        if (session == null){
            session = this.cluster.connect(getKeyspace(domainUri));
            sessions.put(domainUri,session);
        }

        return session;
    }

    public Session getSession(){
        if (controlSession == null){
            controlSession =  this.cluster.connect();
        }
        return controlSession;
    }

    public void closeSession(String domainUri){
        if (sessions.contains(domainUri)){
            sessions.get(domainUri).close();
            sessions.remove(domainUri);
        }
    }

    public void closeSession(){
        if (this.controlSession != null){
            this.controlSession.close();
        }
    }

    public String getKeyspace(String domainUri){
        return new StringBuilder("lda_").append(URIGenerator.retrieveId(domainUri)).toString();
    }

    public static String getKeyspaceFromId(String id){
        return new StringBuilder("lda_").append(id).toString();
    }

    public static String getKeyspaceFromUri(String uri){
        return new StringBuilder("lda_").append(URIGenerator.retrieveId(uri)).toString();
    }

}
