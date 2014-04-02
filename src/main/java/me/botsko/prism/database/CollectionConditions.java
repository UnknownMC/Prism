package me.botsko.prism.database;

import me.botsko.prism.actionlibs.QueryParameters;

import com.mongodb.BasicDBObject;

public class CollectionConditions {
    
    
    /**
     * 
     * @return
     */
    public static BasicDBObject queryParamsToMongo( QueryParameters parameters ){
        
        BasicDBObject query = new BasicDBObject();
        

           query.append( "action", new BasicDBObject("$in", parameters.getActionTypeNames().keySet()) );
        
        
            System.out.println( query.toString() );
        return query;
        
    }
}