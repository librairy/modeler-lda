package org.librairy.modeler.lda.helper;

import lombok.Data;
import org.librairy.computing.cluster.Partitioner;
import org.librairy.computing.helper.SparkHelper;
import org.librairy.computing.helper.StorageHelper;
import org.librairy.modeler.lda.builder.*;
import org.librairy.storage.UDM;
import org.librairy.storage.generator.URIGenerator;
import org.librairy.storage.system.column.repository.UnifiedColumnRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Created by cbadenes on 12/01/16.
 */
@Data
@Component
public class ModelingHelper {

    @Value("#{environment['LIBRAIRY_LDA_VOCABULARY_SIZE']?:${librairy.lda.vocabulary.size}}")
    Integer vocabSize;

    @Autowired
    SparkHelper sparkHelper;

    @Autowired
    URIGenerator uriGenerator;

    @Autowired
    LDABuilder ldaBuilder;

    @Autowired
    StorageHelper storageHelper;

    @Autowired
    CorpusBuilder corpusBuilder;

    @Autowired
    Partitioner partitioner;

    @Autowired
    DealsBuilder dealsBuilder;

    @Autowired
    SimilarityBuilder similarityBuilder;

    @Autowired
    TopicsBuilder topicsBuilder;

    @Autowired
    UDM udm;

    @Autowired
    UnifiedColumnRepository columnRepository;
}
