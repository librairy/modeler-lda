package org.librairy.modeler.lda.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.librairy.model.domain.resources.Resource;
import org.librairy.storage.UDM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by cbadenes on 15/03/16.
 */
@Component
public class CacheManager {

    private static final Logger LOG = LoggerFactory.getLogger(CacheManager.class);

    @Autowired
    UDM udm;

    private LoadingCache<String, String> cache;

    @PostConstruct
    public void setup(){
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(120, TimeUnit.MINUTES)
                //.removalListener(MY_LISTENER)
                .build(
                        new CacheLoader<String, String>() {
                            public String load(String key) {
                                List<String> uris = udm.find(Resource.Type.DOMAIN).from(Resource.Type.ITEM, key);
                                return (uris.isEmpty())? null : uris.get(0);
                            }
                        });
    }



    public String getDomain(String itemUri){
        try {
            return cache.get(itemUri);
        } catch (ExecutionException e) {
            throw new RuntimeException("Error reading from internal cache",e);
        }
    }


}
