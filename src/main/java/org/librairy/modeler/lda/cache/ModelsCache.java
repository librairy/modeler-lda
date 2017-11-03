/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.TopicModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class ModelsCache {

    private static final Logger LOG = LoggerFactory.getLogger(ModelsCache.class);

    @Autowired
    ModelingHelper helper;

    private LoadingCache<String, Boolean> cache;

    @PostConstruct
    public void setup(){
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, Boolean>() {
                            public Boolean load(String uri) {
                                String id = URIGenerator.retrieveId(uri);
                                String ldaPath = helper.getStorageHelper().path(id, "lda");
                                return helper.getStorageHelper().exists(ldaPath);
                            }
                        });
    }


    public boolean exists(String domainUri){
        try {
            return this.cache.get(domainUri);
        } catch (ExecutionException e) {
            LOG.error("Error getting model from: " + domainUri);
            return false;
        }
    }


    public TopicModel getModel(ComputingContext context, String domainUri){
        try {
            return loadModelFromSystem(context, domainUri);
        } catch (ExecutionException e) {
            LOG.debug("Error getting domain model from cache", e);
            return null;
        }

    }


    public void update(String domainUri){
        this.cache.refresh(domainUri);
    }



    private synchronized TopicModel loadModelFromSystem(ComputingContext context, String uri) throws ExecutionException{
        String domainId = URIGenerator.retrieveId(uri);
        try{
            return  helper.getLdaBuilder().load(context, domainId);
        }catch (RuntimeException e){
            LOG.debug("Model for domain: '" + uri + "' does not exist!!");
        }
        return null;
    }
}
