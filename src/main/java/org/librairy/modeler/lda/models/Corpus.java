/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.models;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.base.Strings;
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
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.librairy.boot.model.domain.resources.Item;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.storage.dao.DBSessionManager;
import org.librairy.boot.storage.dao.DomainsDao;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.modeler.lda.dao.ShapeRow;
import org.librairy.modeler.lda.dao.ShapesDao;
import org.librairy.modeler.lda.functions.RowToPair;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created on 31/08/16:
 *
 * @author cbadenes
 */
@Data
public class Corpus {

    private static final Logger LOG = LoggerFactory.getLogger(Corpus.class);

    private final List<Resource.Type> types;
    private final String domainUri;
    private final ComputingContext context;

    String id;
    Map<Long, String> registry;
    ModelingHelper helper;
    CountVectorizerModel countVectorizerModel;
    DataFrame df;
    RDD<Tuple2<Object, Vector>> bow;
    private long size;

    public Corpus(ComputingContext context, String id, List<Resource.Type> types, ModelingHelper helper){
        this.context = context;
        this.id = id;
        this.domainUri = URIGenerator.fromId(Resource.Type.DOMAIN, id);
        this.helper = helper;
        this.types = types;

        this.size = 0l;

        for(Resource.Type type: types){
            this.size += helper.getCounterDao().getValue(domainUri, type.route());
        }
    }

    public void clean(){
        if (bow != null) bow.unpersist(false);
        if (df != null) df.unpersist();
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

        JavaRDD<Row> jrdd = context.getSparkContext().parallelize(rows);

        // Define a schema
        StructType schema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField("resource", DataTypes.StringType, false),
                        DataTypes.createStructField(Item.TOKENS, DataTypes.StringType, false)
                });

        DataFrame df = context.getSqlContext().createDataFrame(jrdd, schema);

        this.df = process(df);
    }

    public void loadResources(String domainUri, List<String> uris){

        DataFrame docsDF = readElements(domainUri,uris);

        // Initialize SHAPE table in database;
        JavaRDD<ShapeRow> rows = docsDF
                .toJavaRDD()
                .map(row -> {
                    ShapeRow shapeRow = new ShapeRow();
                    shapeRow.setUri(row.getString(0));
                    shapeRow.setId(RowToPair.from(row.getString(0)));
                    return shapeRow;
                });

        LOG.info("saving elements id to database..");
        context.getSqlContext()
                .createDataFrame(rows, ShapeRow.class)
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",id)))
                .mode(SaveMode.Overwrite)
                .save();
        LOG.info("saved!");

//        DataFrame resourcesInDomaindf = containsDF.
//                join(resourcesDF, containsDF.col("enduri").equalTo(resourcesDF.col("uri")));

        this.df = process(docsDF)
//                .persist(helper.getCacheModeHelper().getLevel());
        ;

//        docsDF.unpersist();

        LOG.info("processing documents ..");
//        this.df.take(1);

    }

    public void loadDomain(String domainUri){

        DataFrame docsDF = types.stream()
                .map(type -> readElements(domainUri, type))
                .reduce((df1, df2) -> df1.unionAll(df2))
                .get()
//                .persist(helper.getCacheModeHelper().getLevel());
                ;

//        docsDF.take(1);

//        this.size = docsDF.count();

        // Initialize SHAPE table in database;
        JavaRDD<ShapeRow> rows = docsDF
                .toJavaRDD()
                .map(row -> {
                    ShapeRow shapeRow = new ShapeRow();
                    shapeRow.setUri(row.getString(0));
                    shapeRow.setId(RowToPair.from(row.getString(0)));
                    return shapeRow;
                });

        LOG.info("saving elements id to database..");
        context.getSqlContext()
                .createDataFrame(rows, ShapeRow.class)
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", ShapesDao.TABLE, "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",id)))
                .mode(SaveMode.Overwrite)
                .save();
        LOG.info("saved!");

//        DataFrame resourcesInDomaindf = containsDF.
//                join(resourcesDF, containsDF.col("enduri").equalTo(resourcesDF.col("uri")));

        this.df = process(docsDF)
//                .persist(helper.getCacheModeHelper().getLevel());
                ;

//        docsDF.unpersist();

        LOG.info("processing documents ..");
//        this.df.take(1);

    }


    private DataFrame readElements(String domainUri, Resource.Type type){
        final Integer partitions = context.getRecommendedPartitions();
        DataFrame pdf = context.getCassandraSQLContext()
                .read()
                .format("org.apache.spark.sql.cassandra")
                .schema(DataTypes
                        .createStructType(new StructField[]{
                                DataTypes.createStructField("resource", DataTypes.StringType, false),
                                DataTypes.createStructField("tokens", DataTypes.StringType, false),
                                DataTypes.createStructField("domain", DataTypes.StringType, false),
                                DataTypes.createStructField("type", DataTypes.StringType, false)

                        }))
                .option("inferSchema", "false") // Automatically infer data types
                .option("charset", "UTF-8")
                .option("mode", "DROPMALFORMED")
                .options(ImmutableMap.of("table", DomainsDao.TABLE_NAME, "keyspace", DBSessionManager.getCommonKeyspaceId()))
                .load()
                .where("domain='" + domainUri + "' and type='" + type.key() + "'")
                .repartition(partitions);//                .cache()
                ;

        List<Row> emptyTokens = pdf.javaRDD().filter(row -> row.getString(1) == null || row.getString(1).trim().equalsIgnoreCase("")).collect();
        LOG.warn("Empty tokens in " + emptyTokens.size() + " " + type.name());
        emptyTokens.forEach( row -> {
            helper.getDomainsDao().updateDomainTokens(domainUri, row.getString(0),"");
        });
        return pdf;
    }

    private DataFrame readElements(String domainUri, List<String> uris){

        Session session = helper.getDbSessionManager().getCommonSession();

        PreparedStatement statement = session.prepare("select resource, tokens, domain, type from librairy.resources_by_domain where domain='"+domainUri+"' and type = ? and resource = ?");
        List<ResultSetFuture> futures = new ArrayList<>();
        List<com.datastax.driver.core.Row> results = new ArrayList<>();
        for (int i=0;i<uris.size();i++){
            String uri = uris.get(i);
            Resource.Type type = URIGenerator.typeFrom(uri);
            ResultSetFuture resultSetFuture = session.executeAsync(statement.bind(type.key(), uri));
            futures.add(resultSetFuture);
            if (i%100 == 0){
                for (ResultSetFuture future : futures){
                    ResultSet rows = future.getUninterruptibly();
                    com.datastax.driver.core.Row row = rows.one();
                    results.add(row);
                }
                futures.clear();
            }
        }

//        for (String uri: uris) {
//            Resource.Type type = URIGenerator.typeFrom(uri);
//            ResultSetFuture resultSetFuture = session.executeAsync(statement.bind(type.key(), uri));
//            futures.add(resultSetFuture);
//        }

        for (ResultSetFuture future : futures){
            ResultSet rows = future.getUninterruptibly();
            com.datastax.driver.core.Row row = rows.one();

            results.add(row);
        }
        LOG.info(results.size() + " elements read from resources_by_domain");
        //create dataframe
        StructType schema = DataTypes
                .createStructType(new StructField[]{
                        DataTypes.createStructField("resource", DataTypes.StringType, false),
                        DataTypes.createStructField("tokens", DataTypes.StringType, false),
                        DataTypes.createStructField("domain", DataTypes.StringType, false),
                        DataTypes.createStructField("type", DataTypes.StringType, false)

                });
        List<Row> data = results.parallelStream().map(r -> RowFactory.create(r.getString(0), r.getString(1), r.getString(2), r.getString(3))).collect(Collectors.toList());

        return context.getCassandraSQLContext().createDataFrame(data,schema);
    }


    private DataFrame process(DataFrame df){
        LOG.info("Splitting each document into words ..");
        DataFrame words = new RegexTokenizer()
                .setPattern("[\\W]+")
                .setMinTokenLength(4) // Filter away tokens with length < 4
                .setInputCol(Item.TOKENS)
                .setOutputCol("words")
                .transform(df);



        List<String> stopwords = new ArrayList<>();

        Optional<String> stopwordsExpression = helper.getParametersDao().get(domainUri, "lda.stopwords");
        if (stopwordsExpression.isPresent() && !Strings.isNullOrEmpty(stopwordsExpression.get())){
            stopwords = Arrays.asList(stopwordsExpression.get().split(","));
        }

        LOG.info("Filtering by stopwords ["+stopwords.size()+"]");
        DataFrame filteredWords = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("filtered")
                .setStopWords(stopwords.toArray(new String[stopwords.size()]))
                .setCaseSensitive(false)
                .transform(words);

        return filteredWords;
    }

    public RDD<Tuple2<Object, Vector>> getBagOfWords(){

        if (bow != null) return bow;

        if (df == null) throw new RuntimeException("No documents in corpus");

        if (countVectorizerModel == null){

            // Train a Count Vectorizer Model based on corpus
            Integer vocabSize = helper.getVocabularyCache().getVocabSize(domainUri);

            LOG.info("Limiting to top "+vocabSize+" most common words and creating a count vector model ..");
            countVectorizerModel = new CountVectorizer()
                    .setInputCol("filtered")
                    .setOutputCol("features")
                    .setVocabSize(vocabSize)
                    .setMinDF(1)    // Specifies the minimum number of different documents a term must appear in to
                    // be included in the vocabulary.
                    .fit(df);
        }

        Tuple2<Object, Vector> tuple = new Tuple2<Object,Vector>(0l, Vectors.dense(new double[]{1.0}));

        final Integer partitions = context.getRecommendedPartitions();
         bow = countVectorizerModel
                .transform(df)
                .select("resource", "features")
                .repartition(partitions)
                .map(new RowToPair(), ClassTag$.MODULE$.<Tuple2<Object, Vector>>apply(tuple.getClass()))
                 .persist(helper.getCacheModeHelper().getLevel());
        ;
        
        return bow;
    }

    public Long getSize(){
        return this.size;
    }

}
