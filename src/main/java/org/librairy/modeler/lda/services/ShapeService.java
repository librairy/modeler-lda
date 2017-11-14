/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.services;

import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.modeler.lda.cache.ModelsCache;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.tasks.LDAIndividualShapingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class ShapeService {

    private static final Logger LOG = LoggerFactory.getLogger(ShapeService.class);

    private ConcurrentHashMap<String,ScheduledFuture<?>> tasksByDomain;
    private ConcurrentHashMap<String,Runnable> tasks;

    private ThreadPoolTaskScheduler threadpool;

    @Autowired
    ModelingHelper helper;

    @Autowired
    ModelsCache modelsCache;

    @Value("#{environment['LIBRAIRY_LDA_MAX_PENDING_SHAPES']?:${librairy.lda.max.pending.shapes}}")
    Integer maxPendingShapes;

    SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ssZ");

    private ConcurrentHashMap<String, Set<String>> pendingShapes;

    @PostConstruct
    public void setup(){
        this.tasksByDomain  = new ConcurrentHashMap<>();
        this.tasks          = new ConcurrentHashMap<>();

        this.threadpool = new ThreadPoolTaskScheduler();
        this.threadpool.setPoolSize(20);
        this.threadpool.initialize();
        this.threadpool.getScheduledThreadPoolExecutor().setRemoveOnCancelPolicy(true);

        this.pendingShapes = new ConcurrentHashMap<String, Set<String>>();

        LOG.info("Shape Service initialized");

    }


    public boolean process(String domainUri, String resourceUri, long delay){

        if (!modelsCache.exists(domainUri)) return false;

        LOG.info("Scheduled shape for " + resourceUri + " from existing topic model (LDA) in domain: " + domainUri +" at " + timeFormatter.format(new Date(System.currentTimeMillis() + delay)));
        ScheduledFuture<?> task = tasksByDomain.get(domainUri);
        if (task != null) {
            task.cancel(true);
        }
        if (!pendingShapes.containsKey(domainUri)){
            pendingShapes.put(domainUri, new TreeSet<>());
        }
        pendingShapes.get(domainUri).add(resourceUri);

        if (pendingShapes.get(domainUri).size() >= maxPendingShapes){
            // execute partial shaping tasks
            LOG.info("Max pending shapes reached ("+maxPendingShapes+"). Executing partial shaping task ..");
            Set<String> uris = pendingShapes.get(domainUri);
            pendingShapes.put(domainUri, new TreeSet<>());
            new LDAIndividualShapingTask(domainUri, helper, uris).run();
        }else{
            task = this.threadpool.schedule(new LDAIndividualShapingTask(domainUri, helper, pendingShapes.get(domainUri)), new Date(System.currentTimeMillis() + delay));
            tasksByDomain.put(domainUri,task);
        }

        return true;
    }

    public void shapeProcessed(String domainUri, String resourceUri){
        this.pendingShapes.get(domainUri).remove(resourceUri);
        Relation relation = new Relation();
        relation.setStartUri(resourceUri);
        relation.setEndUri(domainUri);
        helper.getEventBus().post(Event.from(relation), RoutingKey.of("shape.created"));
        LOG.info("New shape created for " + resourceUri + " in domain: " + domainUri);
    }

}
