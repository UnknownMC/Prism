package me.botsko.prism.actionlibs;

import java.util.ArrayList;
import java.util.List;

import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

import me.botsko.prism.Prism;
import me.botsko.prism.actions.Handler;
import me.botsko.prism.appliers.PrismProcessType;
import me.botsko.prism.commandlibs.Flag;
import me.botsko.prism.database.DBConditions;

public class ActionsQuery {

    /**
	 * 
	 */
    private final Prism plugin;

    /**
	 * 
	 */
    private boolean shouldGroup = false;

    /**
     * 
     * @param plugin
     * @return
     */
    public ActionsQuery(Prism plugin) {
        this.plugin = plugin;
    }

    /**
     * 
     * @return
     */
    public QueryResult lookup(QueryParameters parameters) {
        return lookup( parameters, null );
    }

    /**
     * 
     * @return
     */
    public QueryResult lookup(QueryParameters parameters, CommandSender sender) {

        Player player = null;
        if( sender instanceof Player ) {
            player = (Player) sender;
        }

        // If lookup, determine if we need to group
        shouldGroup = false;
        if( parameters.getProcessType().equals( PrismProcessType.LOOKUP ) ) {
            shouldGroup = true;
            // What to default to
            if( !plugin.getConfig().getBoolean( "prism.queries.lookup-auto-group" ) ) {
                shouldGroup = false;
            }
            // Any overriding flags passed?
            if( parameters.hasFlag( Flag.NO_GROUP ) || parameters.hasFlag( Flag.EXTENDED ) ) {
                shouldGroup = false;
            }
        }

        // Pull results
        final List<Handler> actions = new ArrayList<Handler>();

        // Build conditions based off final args
        final BasicDBObject query = DBConditions.queryParamsToMongo( parameters );

        if( query != null ) {
            try {

                plugin.eventTimer.recordTimedEvent( "query started" );
                
                BasicDBObject matcher = new BasicDBObject("$match",query);
                
                int sortDir = parameters.getSortDirection().equals( "ASC" ) ? 1 : -1;
                BasicDBObject sorter = new BasicDBObject( "$sort", new BasicDBObject("epoch",sortDir).append( "x", 1 ).append( "z", 1 ).append( "y", 1 ).append( "id", sortDir ) );
                BasicDBObject limit = new BasicDBObject( "$limit", parameters.getLimit() );
//                BasicDBObject group = new BasicDBObject("$group", new BasicDBObject("_id",new BasicDBObject("action","$action").append( "player", "$player" )).append( "count", new BasicDBObject("$sum", 1) ) );
                
//                BasicDBObject group = new BasicDBObject("_id","$action").append( "count", "$sum : 1" );
//                Prism.debug(group.toString());
                
                AggregationOutput aggregated = Prism.getMongoCollection().aggregate( matcher, sorter, limit );
                Prism.debug(aggregated.toString());
                
                plugin.eventTimer.recordTimedEvent( "query returned, building results" );
                
                for (DBObject result : aggregated.results()){

                    if( result.get( "action" ) == null ) continue;

                    // Get the action handler
                    final ActionType actionType = Prism.getActionRegistry().getAction( result.get( "action" ).toString() );

                    if( actionType == null ) continue;

                    try {

                        final Handler baseHandler = Prism.getHandlerRegistry().getHandler( actionType.getHandler() );

                        // Set all shared values
                        baseHandler.setPlugin( plugin );
                        baseHandler.setType( actionType );
                        baseHandler.setUnixEpoch( (Long) result.get( "epoch" ) );
                        baseHandler.setPlayerName( (String) result.get( "player" ) );
                        baseHandler.setWorldName( (String) result.get( "world" ) );
                        baseHandler.setX( (Double) result.get( "x" ) );
                        baseHandler.setY( (Double) result.get( "y" ) );
                        baseHandler.setZ( (Double) result.get( "z" ) );
                        baseHandler.setBlockId( (Integer) result.get( "block_id" ) );
                        baseHandler.setBlockSubId( (Integer) result.get( "block_subid" ) );
                        baseHandler.setOldBlockId( (Integer) result.get( "old_block_id" ) );
                        baseHandler.setOldBlockSubId( (Integer) result.get( "old_block_subid" ) );
                        baseHandler.setData( (String) result.get( "data" ) );
                        baseHandler.setMaterialAliases( Prism.getItems() );

                        // Set aggregate counts if a lookup
                        // @todo mongodb
//                        int aggregated = 0;
//                        if( shouldGroup ) {
//                            aggregated = rs.getInt( 14 );
//                        }
//                        baseHandler.setAggregateCount( aggregated );

                        actions.add( baseHandler );

                    } catch ( final Exception e ) {
                        e.printStackTrace();
                    }
                }
            } catch ( final Exception e ) {
                e.printStackTrace();
            }
        }

        // Build result object
        final QueryResult res = new QueryResult( actions, parameters );
        res.setPerPage( parameters.getPerPage() );

        // Cache it if we're doing a lookup. Otherwise we don't
        // need a cache.
        if( parameters.getProcessType().equals( PrismProcessType.LOOKUP ) ) {
            String keyName = "console";
            if( player != null ) {
                keyName = player.getName();
            }
            if( plugin.cachedQueries.containsKey( keyName ) ) {
                plugin.cachedQueries.remove( keyName );
            }
            plugin.cachedQueries.put( keyName, res );
            // We also need to share these results with the -share-with players.
            for ( final CommandSender sharedPlayer : parameters.getSharedPlayers() ) {
                plugin.cachedQueries.put( sharedPlayer.getName(), res );
            }
        }

        plugin.eventTimer.recordTimedEvent( "results object completed" );

        // Return it
        return res;

    }


    /**
     * 
     * @return
     */
    public int delete(QueryParameters parameters) {
        int total_rows_affected = 0, cycle_rows_affected;
        try {
            final BasicDBObject query = DBConditions.queryParamsToMongo( parameters );
            WriteResult result = Prism.getMongoCollection().remove( query );
            cycle_rows_affected = result.getN();
            total_rows_affected += cycle_rows_affected;
        } catch( MongoException e ){
            e.printStackTrace();
        }
        return total_rows_affected;
    }
}