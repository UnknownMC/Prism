package me.botsko.prism.actionlibs;

import java.util.ArrayList;
import java.util.List;

import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import me.botsko.prism.Prism;
import me.botsko.prism.actions.Handler;
import me.botsko.prism.actions.PrismProcessAction;
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
            
//            DBCursor cursor = null;

            try {

                plugin.eventTimer.recordTimedEvent( "query started" );

                // @todo mongodb
//                // Handle dead connections
//                if( conn == null || conn.isClosed() ) {
//                    if( RecordingManager.failedDbConnectionCount == 0 ) {
//                        Prism.log( "Prism database error. Connection should be there but it's not. Leaving actions to log in queue." );
//                    }
//                    RecordingManager.failedDbConnectionCount++;
//                    sender.sendMessage( Prism.messenger
//                            .playerError( "Database connection was closed, please wait and try again." ) );
//                    return new QueryResult( actions, parameters );
//                } else {
//                    RecordingManager.failedDbConnectionCount = 0;
//                }

//                cursor = Prism.getMongoCollection().find(query);
//                cursor.limit( parameters.getLimit() );
//                int sortDir = parameters.getSortDirection().equals( "ASC" ) ? 1 : -1;
//                cursor.sort( new BasicDBObject("epoch",sortDir).append( "x", 1 ).append( "z", 1 ).append( "y", 1 ).append( "id", sortDir ) );
                
//                BasicDBObject finalQuery = new BasicDBObject("$match",query);
//                int sortDir = parameters.getSortDirection().equals( "ASC" ) ? 1 : -1;
//                finalQuery.append( "$sort", new BasicDBObject("epoch",sortDir).append( "x", 1 ).append( "z", 1 ).append( "y", 1 ).append( "id", sortDir ) );
//                finalQuery.append( "$limit", parameters.getLimit() );
//                finalQuery.append( "$group", new BasicDBObject("_id","$action").append( "count", new BasicDBObject("$sum", 1) ) );
//                Prism.debug(finalQuery.toString());
//                
////                BasicDBObject group = new BasicDBObject("_id","$action").append( "count", "$sum : 1" );
////                Prism.debug(group.toString());
//                
//                
//                AggregationOutput aggregated = Prism.getMongoCollection().aggregate( finalQuery );
//                Prism.debug(aggregated.toString());
                
                BasicDBObject matcher = new BasicDBObject("$match",query);
                
                int sortDir = parameters.getSortDirection().equals( "ASC" ) ? 1 : -1;
                BasicDBObject sorter = new BasicDBObject( "$sort", new BasicDBObject("epoch",sortDir).append( "x", 1 ).append( "z", 1 ).append( "y", 1 ).append( "id", sortDir ) );
                BasicDBObject limit = new BasicDBObject( "$limit", parameters.getLimit() );
//                BasicDBObject project = new BasicDBObject( "$project", new BasicDBObject("player",1) );
                BasicDBObject group = new BasicDBObject("$group", new BasicDBObject("_id",new BasicDBObject("action","$action").append( "player", "$player" )).append( "count", new BasicDBObject("$sum", 1) ) );
//                Prism.debug(finalQuery.toString());
                
//                BasicDBObject group = new BasicDBObject("_id","$action").append( "count", "$sum : 1" );
//                Prism.debug(group.toString());
                
                
                AggregationOutput aggregated = Prism.getMongoCollection().aggregate( matcher, sorter, limit );
                Prism.debug(aggregated.toString());
                

                plugin.eventTimer.recordTimedEvent( "query returned, building results" );
                
                for (DBObject result : aggregated.results()){
                    
//                    System.out.println( "has action: " + record.containsField( "action" ) );
//                    System.out.println( "action: " + record.get( "action" ) );
//                    System.out.println( "_id:" + record.containsField( "_id" ) );
//                    System.out.println( "_id:" + record.get( "_id" ) );
                    
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
            } finally {
//                if( cursor != null ) cursor.close();
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
     * @param playername
     */
    public int getUsersLastPrismProcessId(String playername) {
        int id = 0;
     // @todo mongodb
//        Connection conn = null;
//        PreparedStatement s = null;
//        ResultSet rs = null;
//        try {
//
//            final int action_id = Prism.prismActions.get( "prism-process" );
//
//            conn = Prism.dbc();
//
//            if( conn != null && !conn.isClosed() ) {
//                s = conn.prepareStatement( "SELECT id FROM prism_data JOIN prism_players p ON p.player_id = prism_data.player_id WHERE action_id = ? AND p.player = ? ORDER BY id DESC LIMIT 1" );
//                s.setInt( 1, action_id );
//                s.setString( 2, playername );
//                s.executeQuery();
//                rs = s.getResultSet();
//
//                if( rs.first() ) {
//                    id = rs.getInt( "id" );
//                }
//            } else {
//                Prism.log( "Prism database error. getUsersLastPrismProcessId cannot continue." );
//            }
//        } catch ( final SQLException e ) {
//            plugin.handleDatabaseException( e );
//        } finally {
//            if( rs != null )
//                try {
//                    rs.close();
//                } catch ( final SQLException ignored ) {}
//            if( s != null )
//                try {
//                    s.close();
//                } catch ( final SQLException ignored ) {}
//            if( conn != null )
//                try {
//                    conn.close();
//                } catch ( final SQLException ignored ) {}
//        }
        return id;
    }

    /**
     * 
     * @param id
     */
    public PrismProcessAction getPrismProcessRecord(int id) {
        return null;
     // @todo mongodb
//        PrismProcessAction process = null;
//        Connection conn = null;
//        PreparedStatement s = null;
//        ResultSet rs = null;
//        try {
//
//            String sql = "SELECT id, action, epoch, world, player, x, y, z, data FROM prism_data d";
//            // Joins
//            sql += " INNER JOIN prism_players p ON p.player_id = d.player_id ";
//            sql += " INNER JOIN prism_actions a ON a.action_id = d.action_id ";
//            sql += " INNER JOIN prism_worlds w ON w.world_id = d.world_id ";
//            sql += " LEFT JOIN prism_data_extra ex ON ex.data_id = d.id ";
//            sql += " WHERE d.id = ?";
//
//            conn = Prism.dbc();
//
//            if( conn != null && !conn.isClosed() ) {
//                s = conn.prepareStatement( sql );
//                s.setInt( 1, id );
//                s.executeQuery();
//                rs = s.getResultSet();
//
//                if( rs.first() ) {
//                    process = new PrismProcessAction();
//                    // Set all shared values
//                    process.setId( rs.getInt( "id" ) );
//                    process.setType( Prism.getActionRegistry().getAction( rs.getString( "action" ) ) );
//                    process.setUnixEpoch( rs.getString( "epoch" ) );
//                    process.setWorldName( rs.getString( "world" ) );
//                    process.setPlayerName( rs.getString( "player" ) );
//                    process.setX( rs.getInt( "x" ) );
//                    process.setY( rs.getInt( "y" ) );
//                    process.setZ( rs.getInt( "z" ) );
//                    process.setData( rs.getString( "data" ) );
//                }
//            } else {
//                Prism.log( "Prism database error. getPrismProcessRecord cannot continue." );
//            }
//        } catch ( final SQLException e ) {
//            plugin.handleDatabaseException( e );
//        } finally {
//            if( rs != null )
//                try {
//                    rs.close();
//                } catch ( final SQLException ignored ) {}
//            if( s != null )
//                try {
//                    s.close();
//                } catch ( final SQLException ignored ) {}
//            if( conn != null )
//                try {
//                    conn.close();
//                } catch ( final SQLException ignored ) {}
//        }
//        return process;
    }

    /**
     * 
     * @return
     */
    public int delete(QueryParameters parameters) {
        int total_rows_affected = 0, cycle_rows_affected;
//        Connection conn = null;
//        Statement s = null;
//        try {
//         // @todo mongodb
////            final DeleteQueryBuilder dqb = new DeleteQueryBuilder( plugin );
////            // Build conditions based off final args
////            final String query = dqb.getQuery( parameters, shouldGroup );
////            conn = Prism.dbc();
////            if( conn != null && !conn.isClosed() ) {
////                s = conn.createStatement();
////                cycle_rows_affected = s.executeUpdate( query );
////                total_rows_affected += cycle_rows_affected;
////            } else {
////                Prism.log( "Prism database error. Purge cannot continue." );
////            }
//        } catch ( final SQLException e ) {
//            e.printStackTrace();
//        } finally {
//            if( s != null )
//                try {
//                    s.close();
//                } catch ( final SQLException ignored ) {}
//            if( conn != null )
//                try {
//                    conn.close();
//                } catch ( final SQLException ignored ) {}
//        }
        return total_rows_affected;
    }
}