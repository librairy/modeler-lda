/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.services;

import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.builder.TopicsBuilder;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.tasks.BuildSimilarityTask;
import org.librairy.modeler.lda.tasks.CleanSimilarityTask;
import org.librairy.modeler.lda.tasks.LDACreationTask;
import org.librairy.modeler.lda.tasks.LDAModelingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class SimilarityService {

    private static final Logger LOG = LoggerFactory.getLogger(SimilarityService.class);

    private ConcurrentHashMap<String,ScheduledFuture<?>> cleaningTasks;
    private ConcurrentHashMap<String,ScheduledFuture<?>> buildingTasks;

    private ThreadPoolTaskScheduler threadpool;

    @Autowired
    ModelingHelper helper;

    @Autowired
    TopicsBuilder topicsBuilder;

    SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ssZ");

    @PostConstruct
    public void setup(){
        this.buildingTasks = new ConcurrentHashMap<>();
        this.cleaningTasks = new ConcurrentHashMap<>();

        this.threadpool = new ThreadPoolTaskScheduler();
        this.threadpool.setPoolSize(500);
        this.threadpool.initialize();
    }


    public void discover(String domainUri, Resource.Type resourceType, long delay){
        LOG.debug("Scheduled creation of topic model-based similarities between "+resourceType.route()+" in domain: "+
                domainUri +
                " at " + timeFormatter.format(new Date(System.currentTimeMillis() + delay)));
        ScheduledFuture<?> task = buildingTasks.get(domainUri+resourceType.name());
        if (task != null){
            task.cancel(true);
            this.threadpool.getScheduledThreadPoolExecutor().purge();
        }
        task = this.threadpool.schedule(new BuildSimilarityTask(domainUri,resourceType, helper), new Date(System
                    .currentTimeMillis() + delay));
        buildingTasks.put(domainUri+resourceType.name(),task);
    }


    public void clean(String domainUri, long delay){
        LOG.debug("Scheduled cleaning similar-to relations in domain: " + domainUri + " at " + timeFormatter.format(new
                Date(System.currentTimeMillis() + delay)));
        ScheduledFuture<?> task = cleaningTasks.get(domainUri);
        if (task != null) {
            task.cancel(true);
            this.threadpool.getScheduledThreadPoolExecutor().purge();
        }
        task = this.threadpool.schedule(new CleanSimilarityTask(domainUri,helper), new Date(System.currentTimeMillis() + delay));
        cleaningTasks.put(domainUri,task);
    }

}
