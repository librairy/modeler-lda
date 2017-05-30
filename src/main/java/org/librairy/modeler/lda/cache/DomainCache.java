/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.cache;

import com.datastax.driver.core.Row;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.librairy.boot.model.domain.resources.Domain;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.storage.dao.DomainsDao;
import org.librairy.boot.storage.dao.ItemsDao;
import org.librairy.boot.storage.dao.PartsDao;
import org.librairy.boot.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class DomainCache {

    private static final Logger LOG = LoggerFactory.getLogger(DomainCache.class);

    @Autowired
    ItemsDao itemsDao;

    @Autowired
    PartsDao partsDao;

    LoadingCache<String, List<Domain>> cache;

    @PostConstruct
    public void setup(){
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(2000)
                .expireAfterWrite(10, TimeUnit.SECONDS)
                .build(
                        new CacheLoader<String, List<Domain>>() {
                            public List<Domain> load(String uri) {

                                Resource.Type type = URIGenerator.typeFrom(uri);

                                switch (type){
                                    case ITEM: return itemsDao.listDomains(uri,100,Optional.empty(),true);
                                    case PART: return partsDao.listDomains(uri,100,Optional.empty());
                                    default: return Collections.emptyList();
                                }

                            }
                        });

    }


    public List<Domain> getDomainsFrom(String uri){
        try {
            List<Domain> domains = cache.get(uri);
            if (!domains.isEmpty()) return domains;

            cache.refresh(uri);
            return cache.get(uri);
        } catch (ExecutionException e) {
            LOG.warn("Error reading cache", e);
            return Collections.emptyList();
        }
    }

}
