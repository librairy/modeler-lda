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
import org.librairy.boot.storage.dao.ParametersDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class OptimizerCache {

    private static final Logger LOG = LoggerFactory.getLogger(OptimizerCache.class);

    @Value("#{environment['LIBRAIRY_LDA_OPTIMIZER']?:'${librairy.lda.optimizer}'}")
    String value;

    @Autowired
    ParametersDao parametersDao;

    private LoadingCache<String, String> cache;

    @PostConstruct
    public void setup(){
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, String>() {
                            public String load(String domainUri) {
                                try {
                                    return parametersDao.get(domainUri, "lda.optimizer");
                                } catch (Exception dataNotFound) {
                                    LOG.error("Error reading parameters from '" + domainUri + "'");
                                    return value;
                                }
                            }
                        });
    }


    public String getOptimizer(String domainUri)  {
        try {
            return this.cache.get(domainUri);
        } catch (ExecutionException e) {
            LOG.error("error getting value from database, using default", e);
            return value;
        }
    }

}
