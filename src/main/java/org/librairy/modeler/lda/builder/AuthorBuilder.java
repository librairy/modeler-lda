package org.librairy.modeler.lda.builder;

import com.google.common.base.Strings;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.lang.StringUtils;
import org.librairy.model.domain.resources.User;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by cbadenes on 12/01/16.
 */
@Component
public class AuthorBuilder {


    //TODO this class will dissapear when create Users as entity domains
    public List<User> composeFromMetadata(String authoredBy){

        if (Strings.isNullOrEmpty(authoredBy)) return Collections.emptyList();

        List <User> users = new ArrayList<>();

        for (String author : authoredBy.split(";")){
            User user = new User();
            user.setName(StringUtils.substringAfter(author,","));
            user.setSurname(StringUtils.substringBefore(author,","));
            user.setUri(composeUri(user.getName(),user.getSurname()));
            users.add(user);
        }



        return users;

    }

    private String composeUri(String name, String surname){
        try{
            return "http://librairy.org/users/" + URIUtil.encodeQuery(name)+ "-" +URIUtil.encodeQuery(surname);
        }catch (Exception e){
            return "http://librairy.org/users/default";
        }
    }

}
