package org.librairy.modeler.lda.builder;

import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.librairy.model.domain.relations.DealsWith;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.TopicModel;
import org.librairy.storage.UDM;
import org.librairy.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
@Component
public class DealsBuilder {

    private static Logger LOG = LoggerFactory.getLogger(DealsBuilder.class);

    @Autowired
    UDM udm;

    @Autowired
    URIGenerator uriGenerator;


    public void build(Corpus corpus, TopicModel topicModel, Map<String,String> topicRegistry){

        String domainUri = uriGenerator.from(Resource.Type.DOMAIN, corpus.getId());

        LOG.info("Linking "+corpus.getType().route()+" to topics in domain: " + domainUri);

        RDD<Tuple2<Object, Vector>> documents = corpus.getDocuments().cache();

        LocalLDAModel localLDAModel = topicModel.getLdaModel();

        // Topics distribution
        RDD<Tuple2<Object, Vector>> itemsDistribution = localLDAModel.topicDistributions(documents);


        Tuple2<Object, Vector>[] itemsArray = (Tuple2<Object, Vector>[]) itemsDistribution.collect();

        Map<Object, String> registry = corpus.getRegistry();
        Arrays.stream(itemsArray).parallel().forEach(distribution -> {

            String uri = registry.get(distribution._1);
            double[] weights = distribution._2.toArray();
            for (int i = 0; i< weights.length; i++ ){

                String topicUri = topicRegistry.get(String.valueOf(i));
                DealsWith dealsWith;
                switch(corpus.getType()){
                    case ITEM:
                        dealsWith = Relation.newDealsWithFromItem(uri,topicUri);
                        break;
                    case PART:
                        dealsWith = Relation.newDealsWithFromPart(uri,topicUri);
                        break;
                    case DOCUMENT:
                        dealsWith = Relation.newDealsWithFromDocument(uri,topicUri);
                        break;
                    default: continue;
                }
                dealsWith.setWeight(weights[i]);
                LOG.info("Saving: " + dealsWith);
//                udm.save(dealsWith);
            }
        });

    }
}
