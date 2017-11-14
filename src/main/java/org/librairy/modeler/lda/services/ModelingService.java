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

    private ConcurrentHashMap<String,Boolean> pendingModeling;

    private ThreadPoolTaskScheduler threadpool;

    @Autowired
    ModelingHelper helper;

    SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ssZ");

    @PostConstruct
    public void setup(){
        this.buildingTasks      = new ConcurrentHashMap<>();
        this.pendingModeling    = new ConcurrentHashMap<>();

        this.threadpool = new ThreadPoolTaskScheduler();
        this.threadpool.setPoolSize(20);
        this.threadpool.initialize();
        this.threadpool.getScheduledThreadPoolExecutor().setRemoveOnCancelPolicy(true);
    }


    public boolean train(String domainUri, long delay){

        try{
            if (!isPendingModeling(domainUri)) return false;
            LOG.info("Scheduled creation of a new topic model (LDA) for the domain: " + domainUri + " at " + timeFormatter
                    .format(new Date(System.currentTimeMillis() + delay)));
            ScheduledFuture<?> task = buildingTasks.get(domainUri);
            if (task != null) {
                task.cancel(true);
            }
            task = this.threadpool.schedule(new LDATrainingTask(domainUri, helper), new Date(System.currentTimeMillis() + delay));
            buildingTasks.put(domainUri,task);
            return true;
        }catch (Exception e){
            LOG.error("Error scheduling a new model for domain: " + domainUri, e);
            return true;
        }
    }


    public void enablePendingModelingFor(String domainUri){
        LOG.info("Enable topic model creation for domain: " + domainUri);
        this.pendingModeling.put(domainUri, true);
    }

    public void disablePendingModelingFor(String domainUri){
        LOG.info("Disable topic model creation for domain: " + domainUri);
        this.pendingModeling.put(domainUri, false);
    }


    public Boolean isPendingModeling(String domainUri){
        return pendingModeling.containsKey(domainUri) && pendingModeling.get(domainUri);
    }

}
