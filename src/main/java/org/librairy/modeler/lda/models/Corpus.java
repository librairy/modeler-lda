/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.models;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.common.collect.ImmutableMap;
import lombok.Data;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.librairy.boot.model.domain.resources.Item;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.storage.dao.DBSessionManager;
import org.librairy.boot.storage.exception.DataNotFound;
import org.librairy.modeler.lda.api.SessionManager;
import org.librairy.modeler.lda.dao.ShapeRow;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.functions.RowToPair;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.boot.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created on 31/08/16:
 *
 * @author cbadenes
 */
@Data
public class Corpus {

    private static final Logger LOG = LoggerFactory.getLogger(Corpus.class);

    private static final int partitions = Runtime.getRuntime().availableProcessors() * 3;

    private final List<Resource.Type> types;
    private final String domainUri;

    String id;
    Map<Long, String> registry;
    ModelingHelper helper;
    CountVectorizerModel countVectorizerModel;
    DataFrame df;
    RDD<Tuple2<Object, Vector>> bow;
    private long size;

    public Corpus(String id, List<Resource.Type> types, ModelingHelper helper){
        this.id = id;
        this.domainUri = URIGenerator.fromId(Resource.Type.DOMAIN, id);
        this.helper = helper;
        this.types = types;
    }

    public void updateRegistry(List<String> ids){
        this.registry = new ConcurrentHashMap<>();
        ids.stream().forEach(id -> registry.put(RowToPair.from(id),id));
    }

    public void loadTexts(List<Text> texts){

        List<String> ids = texts
                .parallelStream()
                .map(text -> text.getId())
                .collect(Collectors.toList());
        updateRegistry(ids);

        List<Row> rows = texts.parallelStream()
                .map(text -> RowFactory.create(text.getId(), text.getContent()))
                .collect(Collectors.toList());

        JavaRDD<Row> jrdd = helper.getSparkHelper().getContext().parallelize(rows);

        // Define a schema
        StructType schema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField(Resource.URI, DataTypes.StringType, false),
                        DataTypes.createStructField(Item.TOKENS, DataTypes.StringType, false)
                });

        DataFrame df = helper.getSqlHelper().getContext().createDataFrame(jrdd, schema);

        this.df = process(df);
    }

    public void loadDomain(String domainUri){

        DataFrame docsDF = types.stream()
                .map(type -> readElements(domainUri, type))
                .reduce((df1, df2) -> df1.unionAll(df2))
                .get()
                .cache()
                ;

        docsDF.take(1);

        this.size = docsDF.count();

        // Initialize SHAPE table in database;
        JavaRDD<ShapeRow> rows = docsDF
                .toJavaRDD()
                .map(row -> {
                    ShapeRow shapeRow = new ShapeRow();
                    shapeRow.setUri(row.getString(0));
                    shapeRow.setId(RowToPair.from(row.getString(0)));
                    return shapeRow;
                });

        LOG.info("saving "+this.size+" elements id to database..");
        CassandraJavaUtil.javaFunctions(rows)
                .writerBuilder(SessionManager.getKeyspaceFromId(id), ShapesDao.TABLE, mapToRow(ShapeRow.class))
                .saveToCassandra();
        LOG.info("saved!");

//        DataFrame resourcesInDomaindf = containsDF.
//                join(resourcesDF, containsDF.col("enduri").equalTo(resourcesDF.col("uri")));

        this.df = process(docsDF)
                .cache();

        LOG.info("processing documents ..");
        this.df.take(1);

    }


    private DataFrame readElements(String domainUri, Resource.Type type){
        return helper.getCassandraHelper().getContext()
                .read()
                .format("org.apache.spark.sql.cassandra")
                .schema(DataTypes
                        .createStructType(new StructField[] {
                                DataTypes.createStructField("uri", DataTypes.StringType, false),
                                DataTypes.createStructField("tokens", DataTypes.StringType, false)
                        }))
                .option("inferSchema", "false") // Automatically infer data types
                .option("charset", "UTF-8")
                .option("mode","DROPMALFORMED")
                .options(ImmutableMap.of("table", type.route(), "keyspace", DBSessionManager.getKeyspaceFromUri(domainUri)))
                .load()
                .repartition(partitions)
//                .cache()
                ;
    }


    private DataFrame process(DataFrame df){
        LOG.info("Splitting each document into words ..");
        DataFrame words = new RegexTokenizer()
                .setPattern("[\\W_]+")
                .setMinTokenLength(4) // Filter away tokens with length < 4
                .setInputCol(Item.TOKENS)
                .setOutputCol("words")
                .transform(df);

        String stopwordPath = helper.getStorageHelper().path(id,"stopwords.txt");
        List<String> stopwords = helper.getStorageHelper().exists(stopwordPath)?
                helper.getSparkHelper()
                        .getContext()
                        .textFile(helper.getStorageHelper().absolutePath(stopwordPath))
                        .collect() : Collections.EMPTY_LIST;
        LOG.info("Filtering by stopwords ["+stopwords.size()+"]");
        DataFrame filteredWords = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("filtered")
                .setStopWords(stopwords.toArray(new String[]{}))
                .setCaseSensitive(false)
                .transform(words);

        return filteredWords;
    }

    public RDD<Tuple2<Object, Vector>> getBagOfWords(){

        if (bow != null) return bow;

        if (df == null) throw new RuntimeException("No documents in corpus");

        if (countVectorizerModel == null){

            // Train a Count Vectorizer Model based on corpus
            Integer vocabSize;
            try{
                vocabSize = Integer.valueOf(helper.getParametersDao().get(domainUri,"lda.vocabulary.size"));
            } catch (DataNotFound dataNotFound) {
                vocabSize = Integer.valueOf(helper.getVocabSize());
            }

            LOG.info("Limiting to top "+vocabSize+" most common words and creating a count vector model ..");
            countVectorizerModel = new CountVectorizer()
                    .setInputCol("filtered")
                    .setOutputCol("features")
                    .setVocabSize(vocabSize)
                    .setMinDF(1)    // Specifies the minimum number of different documents a term must appear in to
                    // be included in the vocabulary.
                    .fit(df);
        }

        int estimatedPartitions = helper.getPartitioner().estimatedFor(df);
        Tuple2<Object, Vector> tuple = new Tuple2<Object,Vector>(0l, Vectors.dense(new double[]{1.0}));

         bow = countVectorizerModel
                .transform(df)
                .select("uri", "features")
                .repartition(estimatedPartitions)
                .map(new RowToPair(), ClassTag$.MODULE$.<Tuple2<Object, Vector>>apply(tuple.getClass()))
                .cache()
        ;
        
        return bow;
    }

    public Long getSize(){
        return this.size;
    }

}
