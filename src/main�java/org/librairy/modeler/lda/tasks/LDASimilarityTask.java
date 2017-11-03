/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.modeler.lda.dao.ShapeRow;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDASimilarityTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDASimilarityTask.class);

    public static final String ROUTING_KEY_ID = "lda.similarities.created";

    private final ModelingHelper helper;

    private final String domainUri;

    private final Long topics;

    public LDASimilarityTask(String domainUri, ModelingHelper modelingHelper) throws DataNotFound {
        this.domainUri = domainUri;
        this.helper = modelingHelper;
        this.topics = helper.getCounterDao().getValue(domainUri, org.librairy.boot.model.domain.resources.Resource.Type.TOPIC.route());
    }


    @Override
    public void run() {

//        helper.getCounterDao().reset(domainUri, Relation.Type.SIMILAR_TO_ITEMS.route());
//
//        try{
//            //drop similarity tables
//            helper.getSimilaritiesDao().destroy(domainUri);
//            helper.getClusterDao().destroy(domainUri);
//            helper.getShapesDao().destroyCentroids(domainUri);
//
//
//            Optional<Integer> size = Optional.of(100);
//            Optional<Long> offset = Optional.empty();
//
//            while(true){
//
//                List<ShapeRow> shapes = helper.getShapesDao().get(domainUri, size, offset);
//
//                shapes.parallelStream().forEach(shape -> {
//                    Relation relation = new Relation();
//                    relation.setStartUri(shape.getUri());
//                    relation.setEndUri(domainUri);
//                    helper.getEventBus().post(Event.from(relation), RoutingKey.of("shape.created"));
//                });
//
//                if (shapes.size() < size.get()) break;
//
//                offset = Optional.of(shapes.get(size.get()-1).getId());
//            }
//
//
//        } catch (Exception e){
//            // TODO Notify to event-bus when source has not been added
//            LOG.error("Error calculating similarities in domain: " + domainUri, e);
//        }


    }


}
