/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.services;

import com.google.common.base.Strings;
import org.librairy.modeler.lda.cache.ModelsCache;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.tasks.LDAIndividualShapingTask;
import org.librairy.modeler.lda.tasks.LDATrainingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class ShapeService {

    private static final Logger LOG = LoggerFactory.getLogger(ShapeService.class);

    private ConcurrentHashMap<String,ScheduledFuture<?>> tasks;

    private ThreadPoolTaskScheduler threadpool;

    @Autowired
    ModelingHelper helper;

    @Autowired
    ModelsCache modelsCache;

    SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ssZ");

    @PostConstruct
    public void setup(){
        this.tasks = new ConcurrentHashMap<>();

        this.threadpool = new ThreadPoolTaskScheduler();
        this.threadpool.setPoolSize(10000);

        this.threadpool.initialize();
    }


    public boolean process(String domainUri, String resourceUri, long delay){

        if (!modelsCache.exists(domainUri)) return false;

        String uniqueUri = domainUri + "/" + resourceUri;

        LOG.info("Scheduled shape from existing topic model (LDA) for the domain: " + uniqueUri +" at " + timeFormatter.format(new Date(System.currentTimeMillis() + delay)));
        ScheduledFuture<?> task = tasks.get(uniqueUri);
        if (task != null) {
            task.cancel(true);
//            this.threadpool.getScheduledThreadPoolExecutor().purge();
        }
        task = this.threadpool.schedule(new LDAIndividualShapingTask(domainUri, resourceUri, helper), new Date(System.currentTimeMillis() + delay));
        tasks.put(uniqueUri,task);
        return true;
    }

}
