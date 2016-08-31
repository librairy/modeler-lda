package org.librairy.modeler.lda.models;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.librairy.model.domain.resources.Item;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.functions.RowToPair;
import org.librairy.modeler.lda.helper.ModelingHelper;
import org.librairy.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    String id;
    Resource.Type type;
    List<String> uris;
    Integer vocabSize;
    Map<Object,String> registry;
    ModelingHelper helper;
    CountVectorizerModel countVectorizerModel;

    public Corpus(String id, List<String> uris, Integer vocabSize, ModelingHelper helper){
        this.id = id;
        this.uris = uris;
        this.type = URIGenerator.typeFrom(uris.get(0));
        this.helper = helper;
        this.vocabSize = vocabSize;

        this.registry = new ConcurrentHashMap<>();
        uris.parallelStream().forEach(uri -> registry.put(RowToPair.from(uri),uri));
    }


    public DataFrame getDataFrame(){
        // Create a Data Frame from Cassandra query
        CassandraSQLContext cc = new CassandraSQLContext(helper.getSparkHelper().getContext().sc());

        // Define a schema
        StructType schema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField(Resource.URI, DataTypes.StringType, false),
                        DataTypes.createStructField(Item.TOKENS, DataTypes.StringType, false)
                });

        String whereClause = "uri in (" + uris.stream().map(uri -> "'"+uri+"'").collect(Collectors.joining(", ")) + ")";

        DataFrame df = cc
                .read()
                .format("org.apache.spark.sql.cassandra")
                .schema(schema)
                .option("inferSchema", "false") // Automatically infer data types
                .option("charset", "UTF-8")
                .option("mode","DROPMALFORMED")
                .options(ImmutableMap.of("table", type.route(), "keyspace", "research"))
                .load()
                .where(whereClause)
                ;

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

    public RDD<Tuple2<Object, Vector>> getDocuments(){

        DataFrame df = getDataFrame();

        if (countVectorizerModel == null){
            // Train a Count Vectorizer Model based on corpus
            LOG.info("Limiting to top "+vocabSize+" most common words and convert to word count vector features ..");
            countVectorizerModel = new CountVectorizer()
                    .setInputCol("filtered")
                    .setOutputCol("features")
                    .setVocabSize(vocabSize)
                    .setMinDF(5)    // Specifies the minimum number of different documents a term must appear in to be included in the vocabulary.
                    .fit(df);
        }

        int estimatedPartitions = helper.getPartitioner().estimatedFor(df);
        LOG.info("Estimated Partitions set to: " + estimatedPartitions);
        Tuple2<Object, Vector> tuple = new Tuple2<Object,Vector>(0l, Vectors.dense(new double[]{1.0}));

        return countVectorizerModel
                .transform(getDataFrame())
                .select("uri", "features")
                .repartition(estimatedPartitions)
                .map(new RowToPair(), ClassTag$.MODULE$.<Tuple2<Object, Vector>>apply(tuple.getClass()));
    }

    public Integer getSize(){
        return this.uris.size();
    }

}
