package org.librairy.modeler.models.topic.online;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import es.cbadenes.lab.test.IntegrationTest;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.clustering.OnlineLDAOptimizer;
import org.apache.spark.mllib.linalg.Vector;
import org.junit.Before;
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

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created on 25/04/16:
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
public class AbstractEvaluation {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractEvaluation.class);

    @Autowired
    protected SparkHelper sparkHelper;

    @Autowired
    protected UDM udm;

    @Autowired
    protected UnifiedColumnRepository unifiedColumnRepository;

    protected  PatentsReferenceModel refModel;

    protected LoadingCache<String, String> itemDocCache;

    protected LoadingCache<String, String> docItemCache;

    protected UriSet trainingSet;
    protected UriSet testSet;

    protected double ALPHA    = 0.1;
    protected double BETA     = 0.1;

    protected int ITERATIONS  = 100;
    protected int TOPICS      = 100;

    @Before
    public void setup() throws IOException {

        LOG.info("Creating initial item-cache...");
        // Cache
        this.itemDocCache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(1, TimeUnit.DAYS)
                .build(
                        new CacheLoader<String, String>() {
                            public String load(String uri) {
                                Iterator<Relation> it = unifiedColumnRepository.findBy(Relation.Type.BUNDLES,
                                        "item",
                                        uri).iterator();
                                if (!it.hasNext()) {
                                    return "";
                                }
                                return it.next().getStartUri();
                            }
                        });


        LOG.info("Creating initial doc-cache...");
        this.docItemCache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(1, TimeUnit.DAYS)
                .build(
                        new CacheLoader<String, String>() {
                            public String load(String uri) {
                                Iterator<Relation> it = unifiedColumnRepository.findBy(Relation.Type.BUNDLES,
                                        "document",
                                        uri).iterator();
                                if (!it.hasNext()){
                                    List<String> res = udm.find(Resource.Type.ITEM).from(Resource.Type.DOCUMENT, uri);
                                    if (!res.isEmpty()){
                                        udm.save(Relation.newBundles(uri,
                                                res.get(0)));
                                        return res.get(0);
                                    }
                                    return "";
                                }
                                String iuri = it.next().getEndUri();
                                itemDocCache.put(iuri,uri);
                                return iuri;
                            }
                        });


        LOG.info("Initializing json mapper ...");
        ObjectMapper jsonMapper = new ObjectMapper();


        // Test Set
        this.testSet = new UriSet();
        this.refModel = new PatentsReferenceModel();
        Set<String> allUris = refModel.load("http://librairy.org/documents/");
        LOG.info("Reference Model: " + refModel);
        File testSetFile = new File("src/test/resources/test-set.json");
        if (!testSetFile.exists()){
            LOG.info("Composing test-set..");
            testSet.setUris(Collections.list(refModel.references.keys()));
            LOG.info("Writing test-set to json file ..");
            jsonMapper.writeValue(testSetFile,testSet);
        }{
            LOG.info("Reading test-set from json file ..");
            testSet = jsonMapper.readValue(testSetFile,UriSet.class);
        }
        LOG.info("Test-Set size: " + testSet.getUris().size());

        // Training Set
        this.trainingSet = new UriSet();
        File trainingSetFile = new File("src/test/resources/training-set.json");
        if (!trainingSetFile.exists()){
            LOG.info("Composing training-set..");
            trainingSet.setUris(allUris.parallelStream().filter(uri -> refModel.getRefs(uri).isEmpty()).collect
                    (Collectors.toList()));
            LOG.info("Writing training-set  to json file...");
            jsonMapper.writeValue(trainingSetFile,trainingSet);
        }else{
            LOG.info("Reading training-set from json file...");
            trainingSet = jsonMapper.readValue(trainingSetFile,UriSet.class);
        }
        LOG.info("Training-Set size: " + trainingSet.getUris().size());

    }

    protected LocalLDAModel _buildModel(Double alpha, Double beta, Integer numTopics, Integer numIterations, Corpus
            corpus){
        System.out.println("building the model ..");
        LOG.info
                ("====================================================================================================");
        LOG.info(" TRAINING-STAGE: alpha=" + alpha + ", beta=" + beta + ", numTopics=" + numTopics + ", " +
                "numIterations="+numIterations+", corpusSize="  + corpus.getDocuments().size());
        LOG.info
                ("====================================================================================================");

        JavaPairRDD<Long, Vector> trainingBagsOfWords = corpus.getBagsOfWords().cache();

        Instant start = Instant.now();
        // Online LDA Model :: Creation
        // -> Online Optimizer
        Double TAU              =   1.0;  // how downweight early iterations
        Double KAPPA            =   0.5;  // how quickly old information is forgotten
        Double BATCH_SIZE_RATIO  =   Math.min(1.0,2.0 / numIterations + 1.0 / corpus.getDocuments().size());  // how many
        // documents
        OnlineLDAOptimizer onlineLDAOptimizer = new OnlineLDAOptimizer()
                .setMiniBatchFraction(BATCH_SIZE_RATIO)
                .setOptimizeDocConcentration(true)
                .setTau0(TAU)
                .setKappa(KAPPA)
                ;

        LOG.info("Building the model...");
        LDAModel ldaModel = new LDA().
                setAlpha(alpha).
                setBeta(beta).
                setK(numTopics).
                setMaxIterations(numIterations).
                setOptimizer(onlineLDAOptimizer).
                run(trainingBagsOfWords);

        LocalLDAModel localLDAModel = (LocalLDAModel) ldaModel;
        Instant end = Instant.now();

        // Online LDA Model :: Description
        LOG.info("## Online LDA Model :: Description");

        LOG.info("Log-Perplexity: "     + localLDAModel.logPerplexity(trainingBagsOfWords));
        LOG.info("Log-Likelihood: "     + localLDAModel.logLikelihood(trainingBagsOfWords));
        LOG.info("Vocabulary Size: "    + localLDAModel.vocabSize());
        LOG.info("Elapsed Time: "       + ChronoUnit.MINUTES.between(start,end) + "min " + ChronoUnit.SECONDS
                .between(start,end) + "secs");

        return localLDAModel;

    }

    protected Corpus _composeCorpus(List<String> uris){
        return _composeCorpus(uris,null);
    }

    protected Corpus _composeCorpus(List<String> uris, Map<String, Long> refVocabulary) {
        LOG.info("Composing corpus from: " + uris.size() + " documents ...");

//        Stream<Item> items = uris.parallelStream().
//                filter(uri -> udm.exists(Resource.Type.DOCUMENT).withUri(uri)).
//                map(uri -> docItemCache.getUnchecked(uri)).
//                map(uri -> udm.read(Resource.Type.ITEM).byUri(uri)).
//                filter(response -> response.isPresent()).
//                map(response -> response.get().asItem());


//        List<Tuple2<String, Map<String, Long>>> resources = items.parallel()
//                .map(item -> new Tuple2<String, Map<String, Long>>(item.getUri(), BagOfWords.count(Arrays.asList(item.getTokens().split(" ")))))
//                .collect(Collectors.toList());


        List<Tuple2<String, Map<String, Long>>> resources = new ArrayList<>();

        for(String uri : uris){

            String itemUri = null;
            try {
//                LOG.info("Getting item from: " + uri);
                itemUri = docItemCache.get(uri);
                Optional<Resource> res = udm.read(Resource.Type.ITEM).byUri(itemUri);
                if (!res.isPresent()) throw new ExecutionException(new RuntimeException("Item not found"));
                Item item = res.get().asItem();
                //Default tokenizer
                Map<String, Long> bow = BagOfWords.count(Arrays.asList(item.getTokens()));
                resources.add(new Tuple2<>(item.getUri(),bow));
            } catch (ExecutionException e) {
                LOG.warn("Error getting item from document uri: " + uri,e);
            }

        }




        JavaRDD<Tuple2<String, Map<String, Long>>> itemsRDD = sparkHelper.getSc().parallelize(resources);
        itemsRDD.cache();

        LOG.info("Retrieving the Vocabulary...");
        final Map<String, Long> vocabulary = (refVocabulary != null)? refVocabulary :
                itemsRDD.
                        flatMap(resource -> resource._2.keySet()).
                        distinct().
                        zipWithIndex().
                        collectAsMap();
        ;

        LOG.info( vocabulary.size() + " words" );

        LOG.info("Indexing the documents...");
        Map<Long, String> documents = itemsRDD.
                map(resource -> resource._1).
                zipWithIndex().
                mapToPair(x -> new Tuple2<Long, String>(x._2, x._1)).
                collectAsMap();

        LOG.info( documents.size() + " documents" );

        LOG.info("Building the Corpus...");

        JavaPairRDD<Long, Vector> bagsOfWords = itemsRDD.
                map(resource -> BagOfWords.from(resource._2,vocabulary)).
                zipWithIndex().
                mapToPair(x -> new Tuple2<Long, Vector>(x._2, x._1));

        Corpus corpus = new Corpus();
        corpus.setBagsOfWords(bagsOfWords);
        corpus.setVocabulary(vocabulary);
        corpus.setDocuments(documents);
        return corpus;
    }

}
