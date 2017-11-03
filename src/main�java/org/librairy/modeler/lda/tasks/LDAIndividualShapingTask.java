/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.tasks;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.model.domain.resources.Item;
import org.librairy.boot.model.domain.resources.Part;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.dao.DBSessionManager;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.metrics.aggregation.Bernoulli;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.librairy.modeler.lda.dao.*;
import org.librairy.modeler.lda.functions.RowToSimRow;
import org.librairy.modeler.lda.functions.RowToTupleVector;
import org.librairy.modeler.lda.functions.TupleToResourceShape;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.Text;
import org.librairy.modeler.lda.models.TopicModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.parquet.example.Paper.r1;

/**
 * Created on 12/08/16:
 *
 * @author cbadenes
 */
public class LDAIndividualShapingTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LDAIndividualShapingTask.class);

    public static final String ROUTING_KEY_ID = "lda.individual.shape.created";

    private final ModelingHelper helper;

    private final String domainUri;

    private final String resourceUri;

    public LDAIndividualShapingTask(String domainUri, String resourceUri, ModelingHelper modelingHelper) {
        this.domainUri      = domainUri;
        this.resourceUri    = resourceUri;
        this.helper = modelingHelper;
    }


    @Override
    public void run() {

        try{
            String contextId = "lda.ind.shape."+ URIGenerator.retrieveId(domainUri)+"."+URIGenerator.retrieveId(resourceUri);
            final ComputingContext context = helper.getComputingHelper().newContext(contextId);
            helper.getComputingHelper().execute(context, () -> {
                try{
                    LOG.info("Ready to analyze '"+resourceUri+"'..");
                    String domainId = URIGenerator.retrieveId(domainUri);
                    Resource.Type type = URIGenerator.typeFrom(resourceUri);
                    List<Text> texts = new ArrayList<Text>();
                    String partialUri = "";
                    String id = URIGenerator.retrieveId(resourceUri);
                    switch (type){
                        case ITEM:
                            partialUri = "/documents/"+id;
                            texts.add(new Text(URIGenerator.retrieveId(resourceUri), helper.getItemsDao().getTokens(domainUri, resourceUri)));
                            break;
                        case PART:
                            partialUri = "/parts/"+id;
                            texts.add(new Text(URIGenerator.retrieveId(resourceUri), helper.getPartsDao().getTokens(domainUri, resourceUri)));
                            break;
                        case DOMAIN:
                            partialUri = "/subdomains/"+id;
                            Optional<String> offset = Optional.empty();
                            Integer size = 100;
                            Boolean finished = false;
                            // documents
                            while(!finished){
                                List<Item> docs = helper.getDomainsDao().listItems(resourceUri, size, offset, false);
                                if (docs.isEmpty()) break;
                                for (Item item: docs){
                                    String tokens = helper.getItemsDao().getTokens(domainUri, item.getUri());
                                    LOG.debug("Adding doc '" + item.getUri()+"' from subdomain: '" + resourceUri + "'");
                                    if (!Strings.isNullOrEmpty(tokens)) texts.add(new Text(item.getUri(), tokens ));
                                }
                                finished = (docs.size() < size);
                                if (!finished) offset = Optional.of(docs.get(size-1).getUri());
                            }

                            // parts
                            offset = Optional.empty();
                            finished = false;
                            while(!finished){
                                List<Part> parts = helper.getDomainsDao().listParts(resourceUri, size, offset, false);
                                if (parts.isEmpty()) break;
                                for (Part part: parts){
                                    String tokens = helper.getPartsDao().getTokens(domainUri, part.getUri());
                                    LOG.debug("Adding part '" + part.getUri()+"' from subdomain: '" + resourceUri + "'");
                                    if (!Strings.isNullOrEmpty(tokens)) texts.add(new Text(part.getUri(), tokens ));
                                }
                                finished = (parts.size() < size);
                                if (!finished) offset = Optional.of(parts.get(size-1).getUri());
                            }
                            break;

                    }

                    if (texts.isEmpty() || Strings.isNullOrEmpty(texts.get(0).getContent())) {
                        LOG.info("No tokens available for resource: " + resourceUri + " in domain: " + domainUri);
                        return;
                    }

                    final Integer partitions = context.getRecommendedPartitions();

                    // Load existing model
                    TopicModel model = helper.getModelsCache().getModel(context, domainUri);
                    if (model == null) {
                        LOG.warn("No model found by domain: " + domainUri);
                        return;
                    }

                    // Create a corpus with only one document
                    Corpus corpus = new Corpus(context, domainId, Arrays.asList(new Resource.Type[]{type}), helper);

                    final String completeUri = domainUri+partialUri;

                    corpus.loadTexts(texts);

                    // Use of existing vocabulary
                    corpus.setCountVectorizerModel(model.getVocabModel());

                    // LDA Model
                    LocalLDAModel localLDAModel = model.getLdaModel();


                    // Create and Save shape
                    final Tuple2<Object,double[]> shape = localLDAModel
                            .topicDistributions(corpus.getBagOfWords())
                            .toJavaRDD()
                            .map( a -> new Tuple2<Object,double[]>(a._1, a._2.toArray()))
                            .reduce( (a,b) ->  new Tuple2<Object,double[]>(a._1, Bernoulli.apply(a._2,b._2)));

                    corpus.clean();

                    List<Double> topicVector = Doubles.asList(shape._2);

                    final ShapeRow shapeRow = new ShapeRow();
                    shapeRow.setDate(TimeUtils.asISO());
                    shapeRow.setUri(resourceUri);
                    shapeRow.setVector(topicVector);
                    shapeRow.setId(Long.valueOf(id.hashCode()));
                    this.helper.getShapesDao().save(domainUri,shapeRow);

                    // save in lda_.distributions
                    for (int i=0;i<topicVector.size(); i++){

                        String topicUri = domainUri+"/topics/"+i;
                        DistributionRow distributionRow = new DistributionRow(resourceUri, type.key(),topicUri,TimeUtils.asISO(),topicVector.get(i));
                        this.helper.getDistributionsDao().save(domainUri, distributionRow);
                    }
                    LOG.info("Saved topic distributions for: " + completeUri);


//                    // calculate similarities
//                    JavaRDD<SimilarityRow> simRows = context.getCassandraSQLContext()
//                            .read()
//                            .format("org.apache.spark.sql.cassandra")
//                            .schema(DataTypes
//                                    .createStructType(new StructField[]{
//                                            DataTypes.createStructField(ShapesDao.RESOURCE_URI, DataTypes.StringType, false),
//                                            DataTypes.createStructField(ShapesDao.VECTOR, DataTypes.createArrayType(DataTypes.DoubleType), false)
//                                    }))
//                            .option("inferSchema", "false") // Automatically infer data types
//                            .option("charset", "UTF-8")
////                        .option("spark.sql.autoBroadcastJoinThreshold","-1")
//                            .option("mode", "DROPMALFORMED")
//                            .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
//                            .load()
//                            .repartition(partitions)
//                            .toJavaRDD()
//                            .flatMap( new RowToSimRow(shapeRow.getUri(), shape._2))
//                            .filter(el -> el.getScore() > 0.5)
//                            .cache()
//                    ;
//
//                    LOG.info("Calculating similarities for: " + completeUri);
//                    simRows.take(1);// force cache
//
//                    context.getSqlContext().createDataFrame(simRows, SimilarityRow.class)
//                            .write()
//                            .format("org.apache.spark.sql.cassandra")
//                            .options(ImmutableMap.of("table", SimilaritiesDao.TABLE, "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
//                            .mode(SaveMode.Append)
//                            .save();
//
//                    LOG.info("Saved similarities for: " + completeUri);
//
//                    // Increase similarities counter
//                    helper.getCounterDao().increment(domainUri, Relation.Type.SIMILAR_TO_ITEMS.route(), simRows.count());
//
//                    simRows.unpersist();
//
//                    //TODO update similarity graph

                    // Publish an Event
//                    helper.getEventBus().post(Event.from(completeUri), RoutingKey.of(ROUTING_KEY_ID));
//                    LOG.info("Published event for: " + completeUri);

                    LOG.info("Task completed!!!");
                } catch (Exception e){
                    if (e instanceof InterruptedException){ LOG.info("Execution interrupted during process.");}
                    else LOG.error("Error scheduling an individual topic model distribution for Resource: "+ resourceUri +" in domain: " + domainUri, e);
                }
            });
        } catch (InterruptedException e) {
            LOG.info("Execution interrupted.");
        }
        LOG.info("LDA Individual task executed!!!");


    }


}
