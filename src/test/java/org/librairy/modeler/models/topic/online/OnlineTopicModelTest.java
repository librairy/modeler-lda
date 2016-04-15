package org.librairy.modeler.models.topic.online;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import es.cbadenes.lab.test.IntegrationTest;
import es.upm.oeg.epnoi.matching.metrics.distance.JensenShannonDivergence;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.clustering.OnlineLDAOptimizer;
import org.apache.spark.mllib.linalg.Vector;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.resources.Item;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.Config;
import org.librairy.modeler.lda.builder.BagOfWords;
import org.librairy.modeler.lda.helper.SparkHelper;
import org.librairy.storage.UDM;
import org.librairy.storage.system.column.repository.UnifiedColumnRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created on 13/04/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@TestPropertySource(properties = {
        "librairy.comparator.delay = 1000",
        "librairy.cassandra.contactpoints = zavijava.dia.fi.upm.es",
        "librairy.cassandra.port = 5011",
        "librairy.cassandra.keyspace = research",
        "librairy.elasticsearch.contactpoints = zavijava.dia.fi.upm.es",
        "librairy.elasticsearch.port = 5021",
        "librairy.neo4j.contactpoints = zavijava.dia.fi.upm.es",
        "librairy.neo4j.port = 5030",
        "librairy.eventbus.host = localhost",
        "librairy.eventbus.port=5041"})
public class OnlineTopicModelTest {

    private static final Logger LOG = LoggerFactory.getLogger(OnlineTopicModelTest.class);


    @Autowired
    SparkHelper sparkHelper;

    @Autowired
    UDM udm;

    @Autowired
    UnifiedColumnRepository unifiedColumnRepository;

    private PatentsReferenceModel refModel;

    @Before
    public void setup(){
        String domain = "http://librairy.org/domains/default";

//        List<String> uris = udm.find(Resource.Type.ITEM).from(Resource.Type.DOMAIN, domain);

        this.refModel = new PatentsReferenceModel();
        refModel.load("http://librairy.org/documents/");

    }


    @Test
    public void onlineLDA() throws IOException {

        // Cache
        LoadingCache<String, String> itemDocCache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(1, TimeUnit.DAYS)
                .build(
                        new CacheLoader<String, String>() {
                            public String load(String uri) {
                                Iterator<Relation> it = unifiedColumnRepository.findBy(Relation.Type.BUNDLES,
                                        "item",
                                        uri).iterator();
                                if (!it.hasNext()) return null;
                                return it.next().getStartUri();
                            }
                        });


        LoadingCache<String, String> docItemCache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(1, TimeUnit.DAYS)
                .build(
                        new CacheLoader<String, String>() {
                            public String load(String uri) {
                                Iterator<Relation> it = unifiedColumnRepository.findBy(Relation.Type.BUNDLES,
                                        "document",
                                        uri).iterator();
                                if (!it.hasNext()) return null;
                                String iuri = it.next().getEndUri();
                                itemDocCache.put(iuri,uri);
                                return iuri;
                            }
                        });


        // Test Settings

        // -> Sample
        int SAMPLE_SIZE         =   10;  // number of documents used to build the model
        double SAMPLE_RATIO     =   0.25; // proportion of references in the sample corpus
        // -> LDA
        Integer MAX_ITERATIONS  =   100;
        Integer NUM_TOPICS      =   5;    // number of clusters
        Double ALPHA            =  -1.0;  // document concentration
        Double BETA             =  -1.0;  // topic concentration
        // -> Online Optimizer
        Double TAU              =   1.0;  // how downweight early iterations
        Double KAPPA            =   0.5;  // how quickly old information is forgotten
        Double BATCH_SIZE_RATIO  =   Math.min(1.0,2.0 / MAX_ITERATIONS + 1.0 / SAMPLE_SIZE);  // how many documents
        // are used each iteration

        LOG.info("Test settings: \n"
                +"- Sample Size= " + SAMPLE_SIZE
                +"- Sample Ratio= " + SAMPLE_RATIO
                +"- Max Iterations= " + MAX_ITERATIONS
                +"- Num Topics= " + NUM_TOPICS
                +"- Alpha= " + ALPHA
                +"- Beta= " + BETA
                +"- Tau= " + TAU
                +"- Kappa= " + KAPPA
                +"- Batch Size Ratio= " + BATCH_SIZE_RATIO
        );

        PatentsReferenceModel.TestSample refSample = refModel.sampleOf(SAMPLE_SIZE);

        double times = (0.1 / SAMPLE_RATIO)*10.0;
        PatentsReferenceModel.TestSample restSample = refModel.sampleOf(SAMPLE_SIZE*Double.valueOf(times).intValue());

        List<String> uris = refSample.all;
        uris.addAll(restSample.all);

        List<String> existingUris = uris.parallelStream().filter(uri -> udm.exists(Resource.Type.DOCUMENT).withUri
                (uri)).collect
                (Collectors.toList());

        Stream<Item> items = existingUris.parallelStream().
                map(uri -> docItemCache.getUnchecked(uri)).
                map(uri -> udm.read(Resource.Type.ITEM).byUri(uri)).
                filter(response -> response.isPresent()).
                map(response -> response.get().asItem());


        List<Tuple2<String, Map<String, Long>>> resources = items.parallel()
                .map(item -> new Tuple2<String, Map<String, Long>>(item.getUri(), BagOfWords.count(Arrays.asList(item.getTokens().split(" ")))))
                .collect(Collectors.toList());


        JavaRDD<Tuple2<String, Map<String, Long>>> itemsRDD = sparkHelper.getSc().parallelize(resources);
        itemsRDD.cache();

        LOG.info("Retrieving the Vocabulary...");

        Broadcast<Map<String, Long>> broadcastVocabulary = sparkHelper.getSc().broadcast(itemsRDD.
                flatMap(resource -> resource._2.keySet()).
                distinct().
                zipWithIndex().
                collectAsMap());

        LOG.info( broadcastVocabulary.getValue().size() + " words" );

        LOG.info("Indexing the documents...");
        Map<Long, String> documents = itemsRDD.map(resource -> resource._1).
                zipWithIndex().mapToPair(x -> new Tuple2<Long, String>(x._2, x._1)).
                collectAsMap();

        LOG.info( documents.size() + " documents" );

        LOG.info("Building the Corpus...");

        JavaPairRDD<Long, Vector> corpus = itemsRDD.
                map(resource -> BagOfWords.from(resource._2,broadcastVocabulary.getValue())).
                zipWithIndex().
                mapToPair(x -> new Tuple2<Long, Vector>(x._2, x._1));;

        corpus.cache();


        // Online LDA Model :: Creation

        OnlineLDAOptimizer onlineLDAOptimizer = new OnlineLDAOptimizer()
                .setMiniBatchFraction(BATCH_SIZE_RATIO)
                .setOptimizeDocConcentration(true)
                .setTau0(TAU)
                .setKappa(KAPPA)
                ;

        LOG.info("Building the model...");
        LDAModel ldaModel = new LDA().
                setAlpha(ALPHA).
                setBeta(BETA).
                setK(NUM_TOPICS).
                setMaxIterations(MAX_ITERATIONS).
                setOptimizer(onlineLDAOptimizer).
                run(corpus);

        LocalLDAModel localLDAModel = (LocalLDAModel) ldaModel;


        // Online LDA Model :: Description
        LOG.info("## Online LDA Model :: Description");

        LOG.info("Perplexity: " + localLDAModel.logPerplexity(corpus));
        LOG.info("Likelihood: " + localLDAModel.logLikelihood(corpus));


        // Output topics. Each is a distribution over words (matching word count vectors)
        LOG.info("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
                + " words):");


        Map<Long,String> vocabularyInverse = broadcastVocabulary.getValue().entrySet()
                .stream()
                .collect(Collectors.toMap((x->x.getValue()),(y->y.getKey())));

        int index = 0;
        for (Tuple2<int[], double[]> description : ldaModel.describeTopics(10)){

            LOG.info("Topic: " + index++);
            int[] words = description._1;
            double[] density = description._2;

            for (int i=0;i<words.length;i++){
                LOG.info("\t ["+words[i]+"]'" + vocabularyInverse.get(Long.valueOf(""+words[i]))+"': " + density[i] );
            }
        }


        // Online LDA Model :: Inference
        LOG.info("## Online LDA Model :: Inference");

        JavaPairRDD<Long, Vector> topicDistributions = localLDAModel.topicDistributions(corpus);

        topicDistributions.collect().forEach(dist -> LOG.info("'" + itemDocCache.getUnchecked(documents.get(dist._1))
                + "': "+
                dist._2));




        // Online LDA Model :: Similarity based on Jensen-Shannon Divergence
        LOG.info("## Online LDA Model :: Similarity");
        SimMatrix simMatrix = new SimMatrix();

        List<WeightedPair> similarities = topicDistributions.
                cartesian(topicDistributions).
                filter(x -> x._1._1.compareTo(x._2()._1) > 0).
                map(x -> new WeightedPair(documents.get(x._1._1), documents.get(x._2._1), JensenShannonDivergence
                        .apply(x._1()._2.toArray(), x._2()._2.toArray()))).
                collect();

        similarities.forEach(w -> simMatrix.add(w.getWeight(),w.getUri1(),w.getUri2()));


        // Online LDA Model :: Similarity based on Jensen-Shannon Divergence
        LOG.info("## Online LDA Model :: Evaluation");


        Map<String,Double> evalByFit = new HashMap();

        List<String> refPatents = refSample.references;

        refPatents.stream().filter(uri -> existingUris.contains(uri)).forEach(uri ->{
            List<String> refSimilars    = refModel.getRefs(uri).stream().map(refUri -> docItemCache.getUnchecked
                    (refUri)).collect(Collectors.toList());
            List<String> similars       = simMatrix.getSimilarsTo(docItemCache.getUnchecked(uri)).subList(0,
                    SAMPLE_SIZE/3);
            Double fit                  = SortedListMeter.measure(refSimilars,similars);
            LOG.info("Patent: " + uri + " fit in " + fit + "|| Refs:["+refSimilars.stream().map(x -> itemDocCache.getUnchecked
                    (x)).collect(Collectors.toList())+"]  " +
                    "Similars:["+similars.stream().map(z->itemDocCache.getUnchecked(z)).collect(Collectors.toList())+"]");
            // Comparison
            evalByFit.put(uri,fit);
        });
        Double acumRate = evalByFit.entrySet().stream().map(x -> x.getValue()).reduce((x, y) -> x + y).get();
        LOG.info("Global Fit Rate: " + (acumRate/evalByFit.size()));

    }

}
