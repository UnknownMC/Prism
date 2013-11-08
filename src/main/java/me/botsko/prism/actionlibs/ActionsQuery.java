package me.botsko.prism.actionlibs;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectQuery;

import me.botsko.prism.Prism;
import me.botsko.prism.actions.Handler;
import me.botsko.prism.actions.PrismProcessAction;
import me.botsko.prism.appliers.PrismProcessType;
import me.botsko.prism.commandlibs.Flag;
import me.botsko.prism.database.mysql.SelectQueryBuilder;
import me.botsko.prism.database.tables.PrismActions;
import me.botsko.prism.database.tables.PrismData;
import me.botsko.prism.database.tables.PrismDataExtra;
import me.botsko.prism.database.tables.PrismPlayers;
import me.botsko.prism.database.tables.PrismWorlds;

public class ActionsQuery {
	
	/**
	 * 
	 */
	private Prism plugin;
	
	/**
	 * 
	 */
	private SelectQueryBuilder qb;
	
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
		this.qb = new SelectQueryBuilder(plugin);
	}
	
	
	/**
	 * 
	 * @return
	 */
	public QueryResult lookup( QueryParameters parameters ){
		return lookup( parameters, null );
	}
	
	
	/**
	 * 
	 * @return
	 */
	public QueryResult lookup( QueryParameters parameters, CommandSender sender ){
		
		Player player = null;
		if(sender instanceof Player){
			player = (Player) sender;
		}
		
		// If lookup, determine if we need to group
		shouldGroup = false;
		if( parameters.getProcessType().equals(PrismProcessType.LOOKUP)){
			shouldGroup = true;
			// What to default to
			if( !plugin.getConfig().getBoolean("prism.queries.lookup-auto-group") ){
				shouldGroup = false;
			}
			// Any overriding flags passed?
			if( parameters.hasFlag(Flag.NO_GROUP) || parameters.hasFlag(Flag.EXTENDED) ){
				shouldGroup = false;
			}
		}
		
		
		// Pull results
		List<Handler> actions = new ArrayList<Handler>();
		
		// Build conditions based off final args
		SelectQuery<Record> query = qb.getQuery(parameters, shouldGroup);

		if(query != null){
			Result<Record> result = null;
			try {
				
				plugin.eventTimer.recordTimedEvent("query started");

				// Execute our query
				result = query.fetch();

	    		plugin.eventTimer.recordTimedEvent("query returned, building results");

	    		for( Record rs : result ){
	    			
	    			if( rs.getValue( PrismActions.PRISM_ACTIONS.ACTION ) == null ) continue;
	    			
	    			// Get the action handler
	    			ActionType actionType = Prism.getActionRegistry().getAction( rs.getValue( PrismActions.PRISM_ACTIONS.ACTION ) );
	    			
	    			if(actionType == null) continue;
	    			
	    			Handler baseHandler = Prism.getHandlerRegistry().getHandler( actionType.getHandler() );

    				// Set all shared values
	    			baseHandler.setPlugin( plugin );
	    			baseHandler.setType( actionType );
	    			baseHandler.setId( rs.getValue( PrismData.PRISM_DATA.ID ).intValue() );
	    			baseHandler.setUnixEpoch( rs.getValue( PrismData.PRISM_DATA.EPOCH ).longValue() );
	    			baseHandler.setPlayerName( rs.getValue( PrismPlayers.PRISM_PLAYERS.PLAYER ) );
	    			baseHandler.setWorldName( rs.getValue( PrismWorlds.PRISM_WORLDS.WORLD ) );
	    			if( shouldGroup ){
		    			baseHandler.setX( ((BigDecimal)rs.getValue("avg_x")).doubleValue() );
		    			baseHandler.setY( ((BigDecimal)rs.getValue("avg_y")).doubleValue() );
		    			baseHandler.setZ( ((BigDecimal)rs.getValue("avg_z")).doubleValue() );
	    			} else {
	    				baseHandler.setX( rs.getValue( PrismData.PRISM_DATA.X ) );
		    			baseHandler.setY( rs.getValue( PrismData.PRISM_DATA.Y ) );
		    			baseHandler.setZ( rs.getValue( PrismData.PRISM_DATA.Z ) );
	    			}
					baseHandler.setBlockId( rs.getValue( PrismData.PRISM_DATA.BLOCK_ID ) );
					baseHandler.setBlockSubId( rs.getValue( PrismData.PRISM_DATA.BLOCK_SUBID ) );
					baseHandler.setOldBlockId( rs.getValue( PrismData.PRISM_DATA.OLD_BLOCK_ID ) );
					baseHandler.setOldBlockSubId( rs.getValue( PrismData.PRISM_DATA.OLD_BLOCK_SUBID ) );
					baseHandler.setData( rs.getValue( PrismDataExtra.PRISM_DATA_EXTRA.DATA ) );
    				baseHandler.setMaterialAliases( plugin.getItems() );
    				
    				// Set aggregate counts if a lookup
    				int aggregated = 0;
    				if( shouldGroup ){
    					aggregated = (Integer) rs.getValue("counted");
    				}
    				baseHandler.setAggregateCount(aggregated);
    				
    				actions.add(baseHandler);
	    			
	    		}
	            
	        } catch (Exception e) {
	        	e.printStackTrace();
//	        	plugin.handleDatabaseException( e );
	        } finally {
	        }
		}
		
		// Build result object
		QueryResult res = new QueryResult( actions, parameters );
		res.setPerPage( parameters.getPerPage() );
		
		// Cache it if we're doing a lookup. Otherwise we don't
		// need a cache.
		if(parameters.getProcessType().equals(PrismProcessType.LOOKUP)){
			String keyName = "console";
			if( player != null ){
				keyName = player.getName();
			}
			if(plugin.cachedQueries.containsKey(keyName)){
				plugin.cachedQueries.remove(keyName);
			}
			plugin.cachedQueries.put(keyName, res);
			// We also need to share these results with the -share-with players.
			for(CommandSender sharedPlayer : parameters.getSharedPlayers()){
				plugin.cachedQueries.put(sharedPlayer.getName(), res);
			}
		}
		
		plugin.eventTimer.recordTimedEvent("results object completed");
		
		// Return it
		return res;
		
	}
	
	
	/**
	 * 
	 * @param playername
	 */
	public int getUsersLastPrismProcessId( String playername ){
		int id = 0;
		Connection conn = null;
		PreparedStatement s = null;
		ResultSet rs = null;
		try {
			
			int action_id = Prism.prismActions.get("prism-process");
            
			conn = Prism.dbc();
    		s = conn.prepareStatement ("SELECT id FROM prism_data JOIN prism_players p ON p.player_id = prism_data.player_id WHERE action_id = ? AND p.player = ? ORDER BY id DESC LIMIT 0,1");
    		s.setInt(1, action_id);
    		s.setString(2, playername);
    		s.executeQuery();
    		rs = s.getResultSet();

    		if(rs.first()){
    			id = rs.getInt("id");
			}
            
        } catch (SQLException e) {
        	plugin.handleDatabaseException( e );
        } finally {
        	if(rs != null) try { rs.close(); } catch (SQLException e) {}
        	if(s != null) try { s.close(); } catch (SQLException e) {}
        	if(conn != null) try { conn.close(); } catch (SQLException e) {}
        }
		return id;
	}
	
	
	/**
	 * 
	 * @param id
	 */
	public PrismProcessAction getPrismProcessRecord( int id ){
		PrismProcessAction process = null;
		Connection conn = null;
		PreparedStatement s = null;
		ResultSet rs = null;
		try {
			
			int action_id = Prism.prismActions.get("prism-process");
            
			conn = Prism.dbc();
    		s = conn.prepareStatement ("SELECT * FROM prism_actions WHERE action_id = ? AND id = ?");
    		s.setInt(1, action_id);
    		s.setInt(2, id);
    		s.executeQuery();
    		rs = s.getResultSet();

    		if(rs.first()){
    			process = new PrismProcessAction();
    			// Set all shared values
    			process.setId( rs.getInt("id") );
    			process.setType( Prism.getActionRegistry().getAction( rs.getString("action") ) );
//    			process.setUnixEpoch( rs.getString("epoch") ); @todo fix
    			process.setWorldName( rs.getString("world") );
    			process.setPlayerName( rs.getString("player") );
    			process.setX( rs.getInt("x") );
    			process.setY( rs.getInt("y") );
    			process.setZ( rs.getInt("z") );
    			process.setData( rs.getString("data") );
			}
            
        } catch (SQLException e) {
        	plugin.handleDatabaseException( e );
        } finally {
        	if(rs != null) try { rs.close(); } catch (SQLException e) {}
        	if(s != null) try { s.close(); } catch (SQLException e) {}
        	if(conn != null) try { conn.close(); } catch (SQLException e) {}
        }
		return process;
	}
	
	
	/**
	 * 
	 * @return
	 */
	public int delete( QueryParameters parameters ){
		int total_rows_affected = 0, cycle_rows_affected;
//		Connection conn = null;
//		Statement s = null;
//		try {
//			// Build conditions based off final args
//			String query = qb.getQuery(parameters, shouldGroup);
//			conn = Prism.dbc();
//			s = conn.createStatement();
//			cycle_rows_affected = s.executeUpdate (query);
//			total_rows_affected += cycle_rows_affected;
//		} catch (SQLException e) {
//			plugin.handleDatabaseException( e );
//		} finally {
//        	if(s != null) try { s.close(); } catch (SQLException e) {}
//        	if(conn != null) try { conn.close(); } catch (SQLException e) {}
//        }
		return total_rows_affected;
	}
}