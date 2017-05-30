/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.lda.helper;

import lombok.Data;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.storage.dao.CounterDao;
import org.librairy.boot.storage.dao.DBSessionManager;
import org.librairy.boot.storage.dao.DomainsDao;
import org.librairy.boot.storage.dao.ParametersDao;
import org.librairy.computing.cache.CacheModeHelper;
import org.librairy.computing.cluster.Partitioner;
import org.librairy.computing.helper.ComputingHelper;
import org.librairy.computing.helper.StorageHelper;
import org.librairy.modeler.lda.builder.CorpusBuilder;
import org.librairy.modeler.lda.builder.DealsBuilder;
import org.librairy.modeler.lda.builder.LDABuilder;
import org.librairy.modeler.lda.builder.WorkspaceBuilder;
import org.librairy.modeler.lda.cache.OptimizerCache;
import org.librairy.modeler.lda.cache.VocabularyCache;
import org.librairy.modeler.lda.dao.*;
import org.librairy.modeler.lda.optimizers.LDAOptimizerFactory;
import org.librairy.modeler.lda.services.SimilarityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by cbadenes on 12/01/16.
 */
@Data
@Component
public class ModelingHelper {

    @Autowired
    VocabularyCache vocabularyCache;

    @Autowired
    ComputingHelper computingHelper;

    @Autowired
    CacheModeHelper cacheModeHelper;

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
    WorkspaceBuilder workspaceBuilder;

    @Autowired
    EventBus eventBus;

    @Autowired
    DBSessionManager dbSessionManager;

    @Autowired
    ShapesDao shapesDao;

    @Autowired
    CounterDao counterDao;

    @Autowired
    SimilarityService similarityService;

    @Autowired
    DomainsDao domainsDao;

    @Autowired
    TopicsDao topicsDao;

    @Autowired
    ComparisonsDao comparisonsDao;

    @Autowired
    ParametersDao parametersDao;

    @Autowired
    SimilaritiesDao similaritiesDao;

    @Autowired
    ClusterDao clusterDao;

    @Autowired
    OptimizerCache optimizerCache;

    @Autowired
    LDAOptimizerFactory ldaOptimizerFactory;


}
