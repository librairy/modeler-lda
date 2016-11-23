/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class UnifiedExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(UnifiedExecutor.class);

    ThreadPoolExecutor executor;

    @PostConstruct
    public void setup(){

        RejectedExecutionHandler block = new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                try {
                    executor.getQueue().put( r );
                } catch (InterruptedException e) {
                    LOG.error("Error handling multiple tasks",e);
                }
            }
        };

        executor = new ThreadPoolExecutor(
                1,
                1,
                1L,
                TimeUnit.MINUTES,
                new ArrayBlockingQueue(1, true),
                block);
//                new ThreadPoolExecutor.CallerRunsPolicy());
    }


    public void execute(Runnable command){
        this.executor.execute(command);
    }

}
