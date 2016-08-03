package org.librairy.modeler.lda.builder;

import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.clustering.OnlineLDAOptimizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.librairy.model.domain.relations.*;
import org.librairy.model.domain.resources.*;
import org.librairy.model.utils.TimeUtils;
import org.librairy.modeler.lda.functions.RowToPair;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.modeler.lda.helper.SparkHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.similarity.RelationalSimilarity;
import org.librairy.storage.UDM;
import org.librairy.storage.generator.URIGenerator;
import org.librairy.storage.system.column.repository.UnifiedColumnRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.io.*;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
@Component
public class OnlineLDABuilder {

    private static Logger LOG = LoggerFactory.getLogger(OnlineLDABuilder.class);

    @Autowired @Setter
    UDM udm;

    @Autowired @Setter
    UnifiedColumnRepository columnRepository;

    @Autowired @Setter
    URIGenerator uriGenerator;

    @Autowired @Setter
    SparkHelper sparkHelper;

    @Value("${spark.filesystem}") @Setter
    String fileSystemEndpoint;

    @Value("${librairy.modeler.folder}")
    String modelFolder;

    @Value("${librairy.vocabulary.folder}")
    String vocabularyFolder;


    public Corpus createCorpus(String domainURI, Integer vocabSize){
        Corpus corpus = new Corpus();

        // Reading Uris
        LOG.info("Reading item uris..");
        List<String> itemsUri = udm.find(Resource.Type.ITEM).from(Resource.Type.DOMAIN, domainURI);
        if ((itemsUri == null) || (itemsUri.isEmpty()))
            throw new RuntimeException("No Items found in domain: " + domainURI);

        // Load Items
        LOG.info("Loading Items from domain: " + domainURI);
        List<Row> items = itemsUri.parallelStream().
                map(uri -> udm.read(Resource.Type.ITEM).byUri(uri)).
                filter(res -> res.isPresent()).map(res -> (Item) res.get()).
                map(item -> RowFactory.create(item.getUri(), item.getTokens())).
                collect(Collectors.toList());

        // -> preprocess items
        LOG.info("Preprocess Items from domain: " + domainURI);
        DataFrame itemsDF = preprocess(items);

        // -> create corpus
        LOG.info("Creating corpus for domain: " + domainURI);
        CountVectorizerModel cvModel = createCorpus(itemsDF,vocabSize);
        corpus.setModel(cvModel);

        ConcurrentHashMap<Long,String> itemRegistry = new ConcurrentHashMap<>();
        items.parallelStream().forEach(row -> {
            String uri = String.valueOf(row.get(0));
            Long id = RowToPair.from(uri);
            itemRegistry.put(id,uri);
        });

        Tuple2<Object, Vector> tuple = new Tuple2<Object,Vector>(0l, Vectors.dense(new double[]{1.0}));
        RDD<Tuple2<Object, Vector>> documents = cvModel.transform(itemsDF).select("uri",
                "features").map(new RowToPair(), ClassTag$.MODULE$.<Tuple2<Object, Vector>>apply(tuple.getClass()))
                .cache();
        corpus.setDocuments(documents);

        return corpus;

    }


    public void build(String domainUri, Integer vocabSize){

        clean(domainUri);

        // Reading Uris
        LOG.info("Reading item uris..");
        List<String> itemsUri = udm.find(Resource.Type.ITEM).from(Resource.Type.DOMAIN, domainUri);
        if ((itemsUri == null) || (itemsUri.isEmpty()))
            throw new RuntimeException("No Items found in domain: " + domainUri);

        // Reading Parts
        LOG.info("Reading part uris..");
        List<String> partUris = udm.find(Resource.Type.PART).from(Resource.Type.DOMAIN, domainUri);

        // Load Items
        LOG.info("Loading Items from domain: " + domainUri);

        List<Row> items = itemsUri.parallelStream().
                        map(uri -> udm.read(Resource.Type.ITEM).byUri(uri)).
                        filter(res -> res.isPresent()).map(res -> (Item) res.get()).
                        map(item -> RowFactory.create(item.getUri(), item.getTokens())).
                        collect(Collectors.toList());


        // -> preprocess items
        LOG.info("Preprocess Items from domain: " + domainUri);
        DataFrame itemsDF = preprocess(items);

        // -> create corpus
        LOG.info("Creating corpus for domain: " + domainUri);
        CountVectorizerModel cvModel = createCorpus(itemsDF,vocabSize);

        ConcurrentHashMap<Long,String> itemRegistry = new ConcurrentHashMap<>();
        items.parallelStream().forEach(row -> {
            String uri = String.valueOf(row.get(0));
            Long id = RowToPair.from(uri);
            itemRegistry.put(id,uri);
        });

        Tuple2<Object, Vector> tuple = new Tuple2<Object,Vector>(0l, Vectors.dense(new double[]{1.0}));
        RDD<Tuple2<Object, Vector>> documents = cvModel.transform(itemsDF).select("uri",
                "features").map(new RowToPair(), ClassTag$.MODULE$.<Tuple2<Object, Vector>>apply(tuple.getClass()))
                .cache();

        // -> build model
        //Double k = 2*Math.sqrt(items.size()/2);
        Double k = 7.0; //TODO algorithm to discover number of topics
        Integer iteration = 10;
        Double alpha    =  -1.0;
        Double beta     =  -1.0;
        LOG.info("Training LDA model from domain: " + domainUri);
        LDAModel ldaModel = trainModel(k.intValue(), iteration, alpha, beta, documents);
        LocalLDAModel localLDAModel = (LocalLDAModel) ldaModel;

        // save Topics
        LOG.info("Saving topics from domain: " + domainUri);
        ConcurrentHashMap<Integer, String> topics = saveTopics(ldaModel, cvModel, domainUri);


        // Save Model and Vocabulary
        String domainId = URIGenerator.retrieveId(domainUri);
        Path modelPath = Paths.get(modelFolder,domainId);
        Path vocabularyPath = Paths.get(vocabularyFolder,domainId);

        try {
            Files.deleteIfExists(modelPath);
            Files.deleteIfExists(vocabularyPath);

            Files.createDirectories(modelPath);
            Files.createDirectories(vocabularyPath);

            localLDAModel.save(sparkHelper.getSc().sc(),modelPath.toString());
            cvModel.save(vocabularyPath.toAbsolutePath().toString());


        } catch (IOException e) {
            LOG.error("Error persisting the model",e);
        }


        // Items distribution
        LOG.info("Saving item distribution for each topic in domain: " + domainUri);
        RDD<Tuple2<Object, Vector>> itemsDistribution = localLDAModel.topicDistributions(documents);


        Tuple2<Object, Vector>[] itemsArray = (Tuple2<Object, Vector>[]) itemsDistribution.collect();

        Arrays.stream(itemsArray).parallel().forEach( itemDistribution -> {
            String itemUri = itemRegistry.get(itemDistribution._1);
            double[] weights = itemDistribution._2.toArray();
            for (int i = 0; i< weights.length; i++ ){
                String topicUri = topics.get(i);

                DealsWithFromItem deals = Relation.newDealsWithFromItem(itemUri,topicUri);
                deals.setWeight(weights[i]);
//                String itemId   = StringUtils.substringAfterLast(itemUri, "/");
//                String topicId  = StringUtils.substringAfterLast(topicUri, "/");
//                deals.setUri(modelingHelper.getUriGenerator().from(Relation.Type.DEALS_WITH_FROM_ITEM, itemId+topicId));
                udm.save(deals);
            }
        });


        // Parts distribution
        if ((partUris != null) && !partUris.isEmpty()){
            LOG.info("Processing parts from domain: " + domainUri);

            List<Row> parts = partUris.parallelStream().
                    map(uri -> udm.read(Resource.Type.PART).byUri(uri)).
                    filter(res -> res.isPresent()).map(res -> (Part) res.get()).
                    map(resource -> RowFactory.create(resource.getUri(), resource.getTokens())).
                    collect(Collectors.toList());

            ConcurrentHashMap<Long,String> partRegistry = new ConcurrentHashMap<>();
            parts.parallelStream().forEach(row -> {
                String uri = String.valueOf(row.get(0));
                Long id = RowToPair.from(uri);
                partRegistry.put(id,uri);
            });

            LOG.info("Saving part distribution from domain: " + domainUri);
            DataFrame partsDF = preprocess(parts);
            RDD<Tuple2<Object, Vector>> partsAsDocuments = cvModel.transform(partsDF).select("uri",
                    "features").map(new RowToPair(), ClassTag$.MODULE$.<Tuple2<Object, Vector>>apply(tuple.getClass()));

            RDD<Tuple2<Object, Vector>> partsDistribution = localLDAModel.topicDistributions(partsAsDocuments);

            Tuple2<Object, Vector>[] partArray = (Tuple2<Object, Vector>[]) partsDistribution.collect();

            Arrays.stream(partArray).parallel().forEach( partDistribution -> {
                String partUri = partRegistry.get(partDistribution._1);
                double[] weights = partDistribution._2.toArray();
                for (int i = 0; i< weights.length; i++ ){
                    String topicUri = topics.get(i);

                    DealsWithFromPart deals = Relation.newDealsWithFromPart(partUri,topicUri);
                    deals.setWeight(weights[i]);
//                    String partId   = StringUtils.substringAfterLast(partUri, "/");
//                    String topicId  = StringUtils.substringAfterLast(topicUri, "/");
//                    deals.setUri(modelingHelper.getUriGenerator().from(Relation.Type.DEALS_WITH_FROM_PART,
//                            partId+topicId));
                    udm.save(deals);
                }
            });

        }

        // Clean Similarities
        LOG.debug("deleting existing similarities ..");
        udm.find(Relation.Type.SIMILAR_TO_DOCUMENTS).from(Resource.Type.DOMAIN, domainUri).parallelStream().forEach(relation
                -> {
            LOG.debug("Deleting relation SIMILAR_TO_DOCS: " + relation.getUri());
            udm.delete(Relation.Type.SIMILAR_TO_DOCUMENTS).byUri(relation.getUri());
        });

        udm.find(Relation.Type.SIMILAR_TO_ITEMS).from(Resource.Type.DOMAIN, domainUri).parallelStream().forEach(relation
                -> {
            LOG.debug("Deleting relation SIMILAR_TO_ITEMS: " + relation.getUri());
            udm.delete(Relation.Type.SIMILAR_TO_ITEMS).byUri(relation.getUri());
        });

        udm.find(Relation.Type.SIMILAR_TO_PARTS).from(Resource.Type.DOMAIN, domainUri).parallelStream().forEach(relation
                -> {
            LOG.debug("Deleting relation SIMILAR_TO_PARTS: " + relation.getUri());
            udm.delete(Relation.Type.SIMILAR_TO_PARTS).byUri(relation.getUri());
        });


        // Items Similarities
        LOG.info("Calculating similarities between items in domain: " + domainUri);
        calculateSimilaritiesBetweenItems(itemsUri,domainUri);

        // Parts Similarities
        if ((partUris != null) && (!partUris.isEmpty())){
            LOG.info("Calculating similarities between parts in domain: " + domainUri);
            calculateSimilaritiesBetweenParts(partUris,domainUri);
        }


    }


    public void clean(String domainUri){
        // Delete previous Topics
        LOG.info("Deleting existing topics");
        columnRepository.findBy(Relation.Type.EMERGES_IN, "domain", domainUri).forEach(relation -> {
            LOG.info("Deleting topic: " + relation.getStartUri());

            udm.find(Relation.Type.DEALS_WITH_FROM_DOCUMENT).from(Resource.Type.TOPIC, relation.getStartUri())
                    .parallelStream().forEach(rel ->
                    udm.delete(Relation.Type.DEALS_WITH_FROM_DOCUMENT).byUri(rel.getUri()));

            udm.find(Relation.Type.DEALS_WITH_FROM_ITEM).from(Resource.Type.TOPIC, relation.getStartUri())
                    .parallelStream().forEach(rel -> udm.delete(Relation.Type.DEALS_WITH_FROM_ITEM).byUri(rel
                    .getUri()));

            udm.find(Relation.Type.DEALS_WITH_FROM_PART).from(Resource.Type.TOPIC, relation.getStartUri())
                    .parallelStream().forEach(rel -> udm.delete(Relation.Type.DEALS_WITH_FROM_PART).byUri(rel.getUri
                    ()));

            udm.find(Relation.Type.MENTIONS_FROM_TOPIC).from(Resource.Type.TOPIC, relation.getStartUri())
                    .parallelStream().forEach(rel -> udm.delete(Relation.Type.MENTIONS_FROM_TOPIC).byUri(rel
                    .getUri()));

            udm.delete(Relation.Type.EMERGES_IN).byUri(relation.getUri());

            udm.delete(Resource.Type.TOPIC).byUri(relation.getStartUri());

        });
        LOG.info("Deleted existing topics");

    }

    private void calculateSimilaritiesBetweenItems(List<String> items, String domainUri){

        JavaRDD<String> itemsRDD = sparkHelper.getSc().parallelize(items);

        List<Tuple2<String, String>> itemsPair = itemsRDD.cartesian(itemsRDD)
                .filter(x -> x._1().compareTo(x._2()) > 0)
                .collect();

        LOG.info("Calculating similarities...");
        itemsPair.parallelStream().forEach( pair -> {

            List<Relationship> p1 = udm.find(Relation.Type.DEALS_WITH_FROM_ITEM).from(Resource.Type
                    .ITEM, pair._1).stream().map(rel -> new Relationship(rel.getEndUri(), rel.getWeight())).collect
                    (Collectors.toList());
            List<Relationship> p2 = udm.find(Relation.Type.DEALS_WITH_FROM_ITEM).from(Resource.Type
                    .ITEM, pair._2).stream().map(rel -> new Relationship(rel.getEndUri(), rel.getWeight())).collect
                    (Collectors.toList());

            Double similarity = RelationalSimilarity.between(p1, p2);

            LOG.info("Attaching SIMILAR_TO (ITEM) based on " + pair);
            SimilarTo simRel1 = Relation.newSimilarToItems(pair._1, pair._2);
            SimilarTo simRel2 = Relation.newSimilarToItems(pair._2, pair._1);

            simRel1.setWeight(similarity);
            simRel1.setDomain(domainUri);
            udm.save(simRel1);

            simRel2.setWeight(similarity);
            simRel2.setDomain(domainUri);
            udm.save(simRel2);

        });

    }

    private void calculateSimilaritiesBetweenParts(List<String> parts, String domainUri){


        JavaRDD<String> urisRDD = sparkHelper.getSc().parallelize(parts);

        List<Tuple2<String, String>> pairs = urisRDD.cartesian(urisRDD)
                .filter(x -> x._1().compareTo(x._2()) > 0)
                .collect();

        LOG.info("Calculating similarities...");
        pairs.parallelStream().forEach( pair -> {

            List<Relationship> p1 = udm.find(Relation.Type.DEALS_WITH_FROM_PART).from(Resource.Type
                    .PART, pair._1).stream().map(rel -> new Relationship(rel.getEndUri(), rel.getWeight())).collect
                    (Collectors.toList());
            List<Relationship> p2 = udm.find(Relation.Type.DEALS_WITH_FROM_PART).from(Resource.Type
                    .PART, pair._2).stream().map(rel -> new Relationship(rel.getEndUri(), rel.getWeight())).collect
                    (Collectors.toList());

            Double similarity = RelationalSimilarity.between(p1, p2);

            LOG.info("Attaching SIMILAR_TO (PART) based on " + pair);
            SimilarTo simRel1 = Relation.newSimilarToParts(pair._1, pair._2);
            SimilarTo simRel2 = Relation.newSimilarToParts(pair._2, pair._1);

            simRel1.setWeight(similarity);
            simRel1.setDomain(domainUri);
            udm.save(simRel1);

            simRel2.setWeight(similarity);
            simRel2.setDomain(domainUri);
            udm.save(simRel2);

        });

    }

    public DataFrame preprocess(List<Row> rows){

        JavaRDD<Row> jrdd = sparkHelper.getSc().parallelize(rows);

        StructType schema = new StructType(new StructField[]{
                new StructField("uri", DataTypes.StringType, false, Metadata.empty()),
                new StructField("tokens", DataTypes.StringType, false, Metadata.empty())
        });

        SQLContext sqlContext = new SQLContext(sparkHelper.getSc());

        int processors = Runtime.getRuntime().availableProcessors()*2; //2 or 3 times

        int numPartitions = Math.max(processors, rows.size()/processors);

        LOG.info("Num Partitions set to: " + numPartitions);

        DataFrame df = sqlContext.createDataFrame(jrdd, schema).repartition(numPartitions);

        LOG.info("Splitting each document into words ..");
//        DataFrame words = new Tokenizer();
        DataFrame words = new RegexTokenizer()
                .setPattern("[\\W_]+")
                .setMinTokenLength(4) // Filter away tokens with length < 4
                .setInputCol("tokens")
                .setOutputCol("words")
                .transform(df);

        LOG.info("Filter out stopwords");

//        List<String> stopwords = sparkHelper.getSc().textFile(fileSystemEndpoint + "/stopwords.txt").collect();
        DataFrame filteredWords = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("filtered")
//                .setStopWords(stopwords.toArray(new String[]{}))
                .setCaseSensitive(false)
                .transform(words);

        return filteredWords;
    }


    public CountVectorizerModel createCorpus(DataFrame df, Integer vocabSize){
        
        LOG.info("Limiting to top `vocabSize` most common words and convert to word count vector features ..");
        CountVectorizerModel cvModel = new CountVectorizer()
                .setInputCol("filtered")
                .setOutputCol("features")
                .setVocabSize(vocabSize)
                .setMinDF(5)    // Specifies the minimum number of different documents a term must appear in to be included in the vocabulary.
//                .setMinTF(50)   // Specifies the minimumn number of times a term has to appear in a document to be
                // included in the vocabulary.
                .fit(df);

        return cvModel;
    }


    public LDAModel trainModel(Integer topics, Integer iterations, Double alpha, Double beta, RDD<Tuple2<Object,
            Vector>> documents){
        LOG.info("Building a corpus by using bag-of-words ..");

        LOG.info("Configuring LDA ..");
        //double mbf = 2.0 / iterations + 1.0 / size;
        double mbf = 0.8;

        LDA lda = new LDA()
                .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(mbf))
                .setK(topics)
                .setMaxIterations(iterations)
                .setDocConcentration(alpha)
                .setTopicConcentration(beta)
                ;

        LOG.info("Running OnlineLDA optimizer on corpus ..");
        Instant startModel  = Instant.now();
        LDAModel ldaModel   = lda.run(documents);
        Instant endModel    = Instant.now();
        LOG.info("## LDA Model created successfully!!!!");

        LOG.info("#####################################################################################");
        LOG.info("Model Elapsed Time: "       + ChronoUnit.MINUTES.between(startModel,endModel) + "min " + (ChronoUnit
                .SECONDS
                .between(startModel,endModel)%60) + "secs");
        LOG.info("Vocabulary Size: "    + ldaModel.vocabSize());
        LOG.info("Corpus Size: "    + documents.count());
        LOG.info("Num Topics: "    + topics);
        LOG.info("Num Iterations: "    + iterations);
        LOG.info("Alpha: "    + alpha);
        LOG.info("Beta: "    + beta);
        LOG.info("#####################################################################################");




        return ldaModel;
    }


    public void persist(LDAModel ldaModel, CountVectorizerModel cvModel, String domainUri ){
        LOG.info("Persist Model and vocabulary");
        try {
            String sufix = "-"+ StringUtils.substringAfterLast(domainUri,"/");

            String vocabName = "/vocab"+sufix;
            LOG.info("Saving the vocabulary: " + vocabName);
            cvModel.save(fileSystemEndpoint +"/models/"+vocabName);

            String modelName = "/model"+sufix;
            LOG.info("Saving the model: " + modelName);
            ldaModel.save(sparkHelper.getSc().sc(), fileSystemEndpoint +"/models/"+modelName);

        }catch (Exception e){
            if (e instanceof FileAlreadyExistsException) {
                LOG.warn(e.getMessage());
            }else {
                LOG.error("Error saving model", e);
            }
        }
    }


    public ConcurrentHashMap<Integer,String> saveTopics(LDAModel ldaModel, CountVectorizerModel cvModel, String
            domainUri){
        Tuple2<int[], double[]>[] topicIndices = ldaModel.describeTopics(10);
        String[] vocabArray = cvModel.vocabulary();

        ConcurrentHashMap<Integer,String> topicTable = new ConcurrentHashMap<>();
        ConcurrentHashMap<String,String> wordTable = new ConcurrentHashMap<>();

        int index = 0;
        for (Tuple2<int[], double[]> topicDistribution : topicIndices){
            StringBuilder content = new StringBuilder();

            LOG.info("Topic-" + index);
            int[] topicWords = topicDistribution._1;
            double[] weights = topicDistribution._2;


            ConcurrentHashMap<String,Double> wordUris = new ConcurrentHashMap<>();
            for (int i=0; i< topicWords.length;i++){
                int wid = topicWords[i];
                String word     = vocabArray[wid];
                Double weight   = weights[i];
                content = content.append(word).append("(").append(weight).append("),");
                LOG.info("\t"+vocabArray[wid] +"\t:" + weights[i]);

                String wordUri;

                if (wordTable.contains(word)){
                    wordUri = wordTable.get(word);
                }else{
                    List<String> result = udm.find(Resource.Type.WORD).by(Word.CONTENT, word);
                    if (result != null && !result.isEmpty()){
                        wordUri = result.get(0);
                    }else {
                        wordUri= uriGenerator.from(Resource.Type.WORD,word);

                        // Create Word
                        Word wordData = Resource.newWord();
                        wordData.setUri(wordUri);
                        wordData.setCreationTime(TimeUtils.asISO());
                        wordData.setContent(word);
                        udm.save(wordData);
                    }
                }

                wordTable.put(word,wordUri);
                wordUris.put(wordUri,weight);


            }
            LOG.info("------------------------------------------");

            // Save Topic
            Topic topic = Resource.newTopic();
            topic.setAnalysis("");
            topic.setContent(content.toString());
            topic.setUri(uriGenerator.basedOnContent(Resource.Type.TOPIC,topic.getContent()));
            LOG.info("Saving topic: " + topic.getUri() + " => " + topic.getContent());
            udm.save(topic);

            EmergesIn emerges = Relation.newEmergesIn(topic.getUri(), domainUri);
            emerges.setAnalysis("");
            udm.save(emerges);


            topicTable.put(index++,topic.getUri());

            // Relate it to Words
            for (String wordUri: wordUris.keySet()){
                // Relate Topic to Word (mentions)
                MentionsFromTopic mentions = Relation.newMentionsFromTopic(topic.getUri(), wordUri);
                mentions.setWeight(wordUris.get(wordUri));
                udm.save(mentions);
            }
        }
        return topicTable;

    }

    public static  void serialize(Object object, String path) throws IOException {
        FileOutputStream fout = new FileOutputStream(path);
        ObjectOutputStream out = new ObjectOutputStream(fout);
        out.writeObject(object);
        out.close();
        fout.close();
        LOG.info("Object serialized to: " + path);
    }

    public static Object deserialize(String path) throws IOException, ClassNotFoundException {
        FileInputStream fin = new FileInputStream(path);
        ObjectInputStream oin = new ObjectInputStream(fin);
        Object value = oin.readObject();
        oin.close();
        fin.close();
        return value;
    }

}
