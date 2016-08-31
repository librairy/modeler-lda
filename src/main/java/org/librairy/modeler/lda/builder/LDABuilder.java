package org.librairy.modeler.lda.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Setter;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.clustering.OnlineLDAOptimizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.librairy.computing.cluster.Partitioner;
import org.librairy.computing.helper.SparkHelper;
import org.librairy.computing.helper.StorageHelper;
import org.librairy.modeler.lda.models.Corpus;
import org.librairy.modeler.lda.models.TopicModel;
import org.librairy.modeler.lda.utils.SerializerUtils;
import org.librairy.storage.UDM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Created on 26/06/16:
 *
 * @author cbadenes
 */
@Component
public class LDABuilder {

    private static Logger LOG = LoggerFactory.getLogger(LDABuilder.class);

    @Autowired
    UDM udm;

    @Autowired
    SparkHelper sparkHelper;

    @Autowired
    StorageHelper storageHelper;

    @Autowired
    Partitioner partitionHelper;

    @Autowired
    CorpusBuilder corpusBuilder;

    ObjectMapper jsonMapper = new ObjectMapper();

    @Value("${librairy.modeler.maxiterations}") @Setter
    Integer maxIterations;

    @Value("${librairy.vocabulary.size}")
    Integer vocabularySize;

    public TopicModel build(Corpus corpus){

        // Train
        TopicModel model = train(corpus);

        // Persist
        persist(model,corpus.getId());

        return model;

    }


    public TopicModel train(Corpus corpus){

        RDD<Tuple2<Object, Vector>> documents = corpus.getDocuments().cache();

        // -> build model
        //TODO algorithm to discover number of topics
        Integer k = Double.valueOf(2*Math.sqrt(corpus.getSize()/2)).intValue();
        Double alpha    = -1.0;
        Double beta     = -1.0;
        double mbf      = 0.8;

        LDA lda = new LDA()
                .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(mbf))
                .setK(k)
                .setMaxIterations(maxIterations)
                .setDocConcentration(alpha)
                .setTopicConcentration(beta)
                ;

        LOG.info("Building a new LDA Model [k="+k+"|maxIter="+maxIterations+"|alpha="+alpha+"|beta="+beta+"] from "+
                corpus.getSize() + " documents");
        Instant startModel  = Instant.now();
        LDAModel ldaModel   = lda.run(documents);
        Instant endModel    = Instant.now();
        LOG.info("LDA Model created in: "       + ChronoUnit.MINUTES.between(startModel,endModel) + "min " + (ChronoUnit
                .SECONDS.between(startModel,endModel)%60) + "secs");


        LocalLDAModel localLDAModel = (LocalLDAModel) ldaModel;

        TopicModel topicModel = new TopicModel(corpus.getId(),localLDAModel, corpus.getCountVectorizerModel());
        return topicModel;
    }


    public void persist(TopicModel model, String id ){
        try {

            // Save the model
            String modelPath = storageHelper.path(id,"lda/model");
            storageHelper.deleteIfExists(modelPath);

            String absoluteModelPath = storageHelper.absolutePath(modelPath);
            LOG.info("Saving (or updating) the lda model at: " + absoluteModelPath);
            model.getLdaModel().save(sparkHelper.getContext().sc(), absoluteModelPath);

            //Save the vocabulary
            String vocabPath = storageHelper.path(id,"lda/vocabulary");
            storageHelper.deleteIfExists(vocabPath);

            String absoluteVocabPath = storageHelper.absolutePath(vocabPath);
            LOG.info("Saving (or updating) the lda vocab at: " + absoluteVocabPath);

            String tmpDir   = System.getProperty("java.io.tmpdir");

            File vocabFile = Paths.get(tmpDir, "vectorizer-" + id + ".ser").toFile();
            SerializerUtils.serialize(model.getCountVectorizerModel(), vocabFile.getAbsolutePath());
            storageHelper.save(vocabPath+"/CountVectorizerModel.ser", vocabFile);

        }catch (Exception e){
            if (e instanceof java.nio.file.FileAlreadyExistsException) {
                LOG.warn(e.getMessage());
            }else {
                LOG.error("Error saving model", e);
            }
        }

    }

    public TopicModel load(String id){

        try {
            // Load the model
            String path = storageHelper.absolutePath(storageHelper.path(id,"lda/model"));
            LOG.info("loading lda model from :" + path);
            LocalLDAModel localLDAModel = LocalLDAModel.load(sparkHelper.getContext().sc(), path);

            //Load the CountVectorizerModel
            String vectorizerPath = storageHelper.path(id,"lda/vocabulary/CountVectorizerModel.ser");
            File vectorizerFile = storageHelper.read(vectorizerPath);

            CountVectorizerModel countVectorizerModel = (CountVectorizerModel) SerializerUtils.deserialize(vectorizerFile.getAbsolutePath());

            return new TopicModel(id,localLDAModel, countVectorizerModel);
        } catch (URISyntaxException | IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
