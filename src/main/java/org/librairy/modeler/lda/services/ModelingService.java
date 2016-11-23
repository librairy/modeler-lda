/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.services;

import org.librairy.modeler.lda.helper.ModelingHelper;
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
public class ModelingService {

    private static final Logger LOG = LoggerFactory.getLogger(ModelingService.class);

    private ConcurrentHashMap<String,ScheduledFuture<?>> buildingTasks;

    private ThreadPoolTaskScheduler threadpool;

    @Autowired
    ModelingHelper helper;

    SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ssZ");

    @PostConstruct
    public void setup(){
        this.buildingTasks = new ConcurrentHashMap<>();

        this.threadpool = new ThreadPoolTaskScheduler();
        this.threadpool.setPoolSize(500);

        this.threadpool.initialize();
    }


    public void train(String domainUri, long delay){
        LOG.info("Scheduled creation of a new topic model (LDA) for the domain: " + domainUri + " at " + timeFormatter
                .format(new Date(System.currentTimeMillis() + delay)));
        ScheduledFuture<?> task = buildingTasks.get(domainUri);
        if (task != null) {
            task.cancel(true);
            this.threadpool.getScheduledThreadPoolExecutor().purge();
        }
        task = this.threadpool.schedule(new LDATrainingTask(domainUri, helper), new Date(System.currentTimeMillis() + delay));
        buildingTasks.put(domainUri,task);

    }


}
