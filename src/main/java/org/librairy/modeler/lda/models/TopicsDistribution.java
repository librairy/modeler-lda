package org.librairy.modeler.lda.models;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * Created on 02/09/16:
 *
 * @author cbadenes
 */
@Data
public class TopicsDistribution {

    String documentId;

    List<TopicDistribution> topics = new ArrayList<>();

    public void add(String topicUri, Double weight){
        TopicDistribution td = new TopicDistribution();
        td.setTopicUri(topicUri);
        td.setWeight(weight);
        topics.add(td);
    }

}
