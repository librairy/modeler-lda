package org.librairy.modeler.lda.models;

import lombok.Data;

import java.io.Serializable;

/**
 * Created on 01/09/16:
 *
 * @author cbadenes
 */
@Data
public class Text implements Serializable{

    private String id;

    private String content;
}
