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
public class DelayCache {

    private static final Logger LOG = LoggerFactory.getLogger(DelayCache.class);

    @Value("#{environment['LIBRAIRY_LDA_EVENT_DELAY']?:${librairy.lda.event.delay}}")
    protected Long value;

    @Autowired
    ParametersDao parametersDao;

    private LoadingCache<String, Long> cache;

    @PostConstruct
    public void setup(){
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, Long>() {
                            public Long load(String domainUri) {
                                try {
                                    String parameter = parametersDao.get(domainUri, "lda.delay");
                                    return Long.valueOf(parameter);
                                } catch (Exception dataNotFound) {
                                    LOG.error("Error reading parameters from '" + domainUri + "'");
                                    return value;
                                }
                            }
                        });
    }


    public Long getDelay(String domainUri)  {
        try {
            return this.cache.get(domainUri);
        } catch (ExecutionException e) {
            LOG.error("error getting value from database, using default", e);
            return value;
        }
    }

}
