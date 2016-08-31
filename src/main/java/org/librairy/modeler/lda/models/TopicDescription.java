package org.librairy.modeler.lda.models;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * Created on 31/08/16:
 *
 * @author cbadenes
 */
@Data
public class TopicDescription {

    private final String id;

    private List<WordDescription> words = new ArrayList<>();

    public TopicDescription(String id){
        this.id = id;
    }

    public TopicDescription add(WordDescription wordDescription){
        this.words.add(wordDescription);
        return this;
    }

    public TopicDescription add(String word, Double weight){
        this.words.add(new WordDescription(word,weight));
        return this;
    }
}
