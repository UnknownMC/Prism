package me.botsko.prism.database;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.bukkit.Location;
import org.bukkit.util.Vector;

import me.botsko.prism.actionlibs.QueryParameters;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

public class DBConditions {
    
    
    /**
     * 
     * @return
     */
    public static BasicDBObject queryParamsToMongo( QueryParameters parameters ){
        
        BasicDBObject query = new BasicDBObject();
        
        // @todo add support for include/excludes
        
        // Action types
        if( !parameters.getActionTypeNames().isEmpty() ){
            query.append( "action", new BasicDBObject("$in", parameters.getActionTypeNames().keySet()) );
        }
        // @todo exclude internal prism stuff?
        
        // Players
        if( !parameters.getPlayerNames().isEmpty() ){
            query.append( "player", new BasicDBObject("$in", parameters.getPlayerNames().keySet()) );
        }
        
        // @todo:

//        parameters.getEntities()
//        parameters.getId();
//        parameters.getKeyword()
        
        // Blocks
        if( !parameters.getBlockFilters().isEmpty() ){
            BasicDBList or = new BasicDBList();
            for ( final Entry<Integer, Byte> entry : parameters.getBlockFilters().entrySet() ) {
                if( entry.getValue() == 0 ){
                    or.add( new BasicDBObject("block_id",entry.getKey()) );
                } else {
                    or.add( new BasicDBObject("block_id",entry.getKey()).append( "block_subid", entry.getValue() ) );
                }
            }
            query.append( "$or", or );
        }
        
        // Specific coords
        final ArrayList<Location> locations = parameters.getSpecificBlockLocations();
        if( locations.size() > 0 ){
            BasicDBList or = new BasicDBList();
            for ( final Location loc : locations ){
                or.add( new BasicDBObject("x",loc.getBlockX())
                            .append( "y", loc.getBlockY() )
                            .append( "z", loc.getBlockZ() ) );

            }
            query.append( "$or", or );
        }
        
        // Coordinate bounds
        Vector maxLoc = parameters.getMaxLocation();
        Vector minLoc = parameters.getMinLocation();
        if( minLoc != null && maxLoc != null ) {
            query.append( "x", new BasicDBObject("$gt", minLoc.getBlockX()).append( "$lt", maxLoc.getBlockX() ) );
            query.append( "y", new BasicDBObject("$gt", minLoc.getBlockY()).append( "$lt", maxLoc.getBlockY() ) );
            query.append( "z", new BasicDBObject("$gt", minLoc.getBlockZ()).append( "$lt", maxLoc.getBlockZ() ) );
        }

        // Time
        if( !parameters.getIgnoreTime() ){
            if( parameters.getBeforeTime() != null && parameters.getBeforeTime() > 0 ){
                query.append( "epoch", new BasicDBObject("$lt", parameters.getBeforeTime()/1000) );
            }
            if( parameters.getSinceTime() != null && parameters.getSinceTime() > 0 ){
                query.append( "epoch", new BasicDBObject("$gte", parameters.getSinceTime()/1000) );
            }
        }

        // World
        if( parameters.getWorld() != null && !parameters.getWorld().isEmpty() ){
            query.append( "world", parameters.getWorld() );
        }

        return query;
        
    }
}