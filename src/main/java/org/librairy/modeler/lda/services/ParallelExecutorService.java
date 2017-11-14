/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
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
public class ParallelExecutorService {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelExecutorService.class);

    private ConcurrentHashMap<String,ScheduledFuture<?>> buildingTasks;

    private ThreadPoolTaskScheduler threadpool;

    SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ssZ");

    public ParallelExecutorService(){
        this.buildingTasks = new ConcurrentHashMap<>();

        this.threadpool = new ThreadPoolTaskScheduler();
        this.threadpool.setPoolSize(20);
        this.threadpool.initialize();
        this.threadpool.getScheduledThreadPoolExecutor().setRemoveOnCancelPolicy(true);
    }


    public void execute(String domainUri, long delay, Runnable task){
        LOG.info("Scheduled task execution " + task.getClass().getName() + " for the domain: " + domainUri + " at " + timeFormatter.format(new Date(System.currentTimeMillis() + delay)));
        ScheduledFuture<?> futureTask = buildingTasks.get(domainUri);
        if (futureTask != null) {
            futureTask.cancel(true);
        }
        futureTask= this.threadpool.schedule(task, new Date(System.currentTimeMillis() + delay));
        buildingTasks.put(domainUri,futureTask);

    }


}
