package org.librairy.modeler.lda.helper;

import lombok.Data;
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

    @Autowired
    SparkHelper sparkHelper;

    @Autowired
    URIGenerator uriGenerator;

    @Autowired
    OnlineLDABuilder onlineLDABuilder;

    @Autowired
    SimilarityBuilder similarityBuilder;

    @Autowired
    Cleaner cleaner;

    @Autowired
    UDM udm;

    @Autowired
    UnifiedColumnRepository columnRepository;
}
