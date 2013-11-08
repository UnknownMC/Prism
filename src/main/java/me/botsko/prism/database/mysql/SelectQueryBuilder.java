package me.botsko.prism.database.mysql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;

import org.bukkit.Location;
import org.bukkit.util.Vector;
import org.jooq.Condition;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.SelectQuery;
import org.jooq.impl.DSL;
import org.jooq.types.UInteger;

import me.botsko.prism.Prism;
import me.botsko.prism.actionlibs.MatchRule;
import me.botsko.prism.appliers.PrismProcessType;
import me.botsko.prism.database.QueryBuilder;
import me.botsko.prism.database.Tables;
import me.botsko.prism.database.tables.PrismActions;
import me.botsko.prism.database.tables.PrismData;
import me.botsko.prism.database.tables.PrismDataExtra;
import me.botsko.prism.database.tables.PrismPlayers;
import me.botsko.prism.database.tables.PrismWorlds;


public class SelectQueryBuilder extends QueryBuilder {
	

	/**
	 * 
	 * @param plugin
	 */
	public SelectQueryBuilder( Prism plugin ){
		super(plugin);
	}
	
	
	/**
	 * 
	 * @return
	 */
	@Override
	protected SelectQuery<Record> select( SelectQuery<Record> query ){
		
		// Select
		query.addSelect(PrismData.PRISM_DATA.ID);
		query.addSelect(PrismData.PRISM_DATA.EPOCH);
		query.addSelect(PrismActions.PRISM_ACTIONS.ACTION);
		query.addSelect(PrismPlayers.PRISM_PLAYERS.PLAYER);
		query.addSelect(PrismWorlds.PRISM_WORLDS.WORLD);
		
		if( shouldGroup ){
			query.addSelect(PrismData.PRISM_DATA.X.avg().as("avg_x"));
			query.addSelect(PrismData.PRISM_DATA.Y.avg().as("avg_y"));
			query.addSelect(PrismData.PRISM_DATA.Z.avg().as("avg_z"));
		} else {
			query.addSelect(PrismData.PRISM_DATA.X);
			query.addSelect(PrismData.PRISM_DATA.Y);
			query.addSelect(PrismData.PRISM_DATA.Z);
		}
		
		query.addSelect(PrismData.PRISM_DATA.BLOCK_ID);
		query.addSelect(PrismData.PRISM_DATA.BLOCK_SUBID);
		query.addSelect(PrismData.PRISM_DATA.OLD_BLOCK_ID);
		query.addSelect(PrismData.PRISM_DATA.OLD_BLOCK_SUBID);
		query.addSelect(PrismDataExtra.PRISM_DATA_EXTRA.DATA);
		
		if( shouldGroup ){
			query.addSelect(DSL.count().as("counted"));
		}
		
		// From
		query.addFrom(Tables.PRISM_DATA);
		
		// Joins
		query.addJoin(Tables.PRISM_PLAYERS, PrismPlayers.PRISM_PLAYERS.PLAYER_ID.equal(PrismData.PRISM_DATA.PLAYER_ID));
		query.addJoin(Tables.PRISM_ACTIONS, PrismActions.PRISM_ACTIONS.ACTION_ID.equal(PrismData.PRISM_DATA.ACTION_ID));
		query.addJoin(Tables.PRISM_WORLDS, PrismWorlds.PRISM_WORLDS.WORLD_ID.equal(PrismData.PRISM_DATA.WORLD_ID));
		query.addJoin(Tables.PRISM_DATA_EXTRA, JoinType.LEFT_OUTER_JOIN, PrismDataExtra.PRISM_DATA_EXTRA.DATA_ID.equal(PrismData.PRISM_DATA.ID));

		return query;
		
	}
	
	
	/**
	 * 
	 * @return
	 */
	@Override
	protected SelectQuery<Record> where( SelectQuery<Record> query ){
		
		// ID Condition overrides anything else
		int id = parameters.getId();
		if(id > 0){
			query.addConditions( PrismData.PRISM_DATA.ID.eq(UInteger.valueOf( id )) );
		}
		
		// World conditions
		if( !parameters.getProcessType().equals(PrismProcessType.DELETE) && parameters.getWorld() != null ){
			query.addConditions( PrismWorlds.PRISM_WORLDS.WORLD.eq(parameters.getWorld()) );
		}
		
		// Action type
		HashMap<String,MatchRule> action_types = parameters.getActionTypeNames();
		// Make sure none of the prism process types are requested
		boolean containsPrismProcessType = false;
		boolean hasPositiveMatchRule = false;
		if( !action_types.isEmpty() ){

			Condition actionConds = null;
			for (Entry<String,MatchRule> entry : action_types.entrySet()){
				
				if( actionConds == null ){
					actionConds = PrismActions.PRISM_ACTIONS.ACTION.eq(entry.getKey());
				} else {
					actionConds = actionConds.or(PrismActions.PRISM_ACTIONS.ACTION.eq(entry.getKey()));
				}
				
				if( !containsPrismProcessType && entry.getKey().contains("prism") ){
					containsPrismProcessType = true;
				}
				if(entry.getValue().equals(MatchRule.INCLUDE)){
					hasPositiveMatchRule = true;
				}
			}
			if( actionConds != null ) query.addConditions( actionConds );
		}
		// exclude internal stuff
		if( !containsPrismProcessType && !parameters.getProcessType().equals(PrismProcessType.DELETE) && !hasPositiveMatchRule ){
			query.addConditions( PrismActions.PRISM_ACTIONS.ACTION.notLike("prism") );
		}
		
		// Player(s)
		HashMap<String,MatchRule> playerNames = parameters.getPlayerNames();
		Condition playerConds = null;
		for (Entry<String,MatchRule> entry : playerNames.entrySet()){
			
			if( playerConds == null ){
				playerConds = PrismPlayers.PRISM_PLAYERS.PLAYER.eq(entry.getKey());
			} else {
				playerConds = playerConds.or(PrismPlayers.PRISM_PLAYERS.PLAYER.eq(entry.getKey()));
			}
		}
		if( playerConds != null ) query.addConditions( playerConds );
		
		// Radius from loc
		if( !parameters.getProcessType().equals(PrismProcessType.DELETE) || (parameters.getProcessType().equals(PrismProcessType.DELETE) && parameters.getFoundArgs().containsKey("r") ) ){
			Vector minLoc = parameters.getMinLocation();
			Vector maxLoc = parameters.getMaxLocation();
			if(minLoc != null && maxLoc != null ){
				query.addConditions( PrismData.PRISM_DATA.X.between(minLoc.getBlockX(), maxLoc.getBlockX()) );
				query.addConditions( PrismData.PRISM_DATA.Y.between(minLoc.getBlockY(), maxLoc.getBlockY()) );
				query.addConditions( PrismData.PRISM_DATA.Z.between(minLoc.getBlockZ(), maxLoc.getBlockZ()) );
			}
		}
		
		// Blocks
		HashMap<Integer,Byte> blockfilters = parameters.getBlockFilters();
		if(!blockfilters.isEmpty()){
			Collection<Condition> blockConditions = new ArrayList<Condition>();
			for (Entry<Integer,Byte> entry : blockfilters.entrySet()){
				if( entry.getValue() == 0 ){
					blockConditions.add( PrismData.PRISM_DATA.BLOCK_ID.eq(entry.getKey()) );
				} else {
					blockConditions.add( PrismData.PRISM_DATA.BLOCK_ID.eq(entry.getKey()).and( PrismData.PRISM_DATA.BLOCK_SUBID.eq((int)entry.getValue()) ) );
				}
			}
			Condition blockOr = null;
			for( Condition c : blockConditions ){
				if( blockOr == null ){
					blockOr = c;
					continue;
				}
				blockOr = blockOr.or(c);
			}
			if( blockOr != null ) query.addConditions(blockOr);
		}
		
		// Entity
		HashMap<String,MatchRule> entityNames = parameters.getEntities();
		if( entityNames.size() > 0 ){
			Condition entityConds = null;
			for (Entry<String,MatchRule> entry : entityNames.entrySet()){
				if( entityConds == null ){
					entityConds = PrismDataExtra.PRISM_DATA_EXTRA.DATA.like("entity_name\":\""+entry.getKey());
					continue;
				}
				entityConds = entityConds.or( PrismDataExtra.PRISM_DATA_EXTRA.DATA.like("entity_name\":\""+entry.getKey()) );
			}
			if( entityConds != null ) query.addConditions(entityConds);
		}
		
		// Timeframe
		Long time = parameters.getBeforeTime();
		if( time != null && time != 0 ){
			query = buildTimeCondition( query, time, true );
		}
		time = parameters.getSinceTime();
		if( time != null && time != 0 ){
			query = buildTimeCondition( query, time, false );
		}
		
		// Keyword(s)
		String keyword = parameters.getKeyword();
		if(keyword != null){
			query.addConditions( PrismDataExtra.PRISM_DATA_EXTRA.DATA.like(keyword) );
		}
		
		// Specific coords
		ArrayList<Location> locations = parameters.getSpecificBlockLocations();
		if( locations.size() >0 ){
			Collection<Condition> locConditions = new ArrayList<Condition>();
			for ( Location loc : locations ){
				locConditions.add( PrismData.PRISM_DATA.X.eq(loc.getBlockX()).and( PrismData.PRISM_DATA.Y.eq(loc.getBlockY()) ).and( PrismData.PRISM_DATA.Z.eq(loc.getBlockZ()) ) );
			}
			Condition locOr = null;
			for( Condition c : locConditions ){
				if( locOr == null ){
					locOr = c;
					continue;
				}
				locOr = locOr.or(c);
			}
			if( locOr != null ) query.addConditions(locOr);
			
		}
//		
//		
//		// Parent process
//		if(parameters.getParentId() > 0){
//			addCondition( String.format("ex.data = %d", parameters.getParentId()) );
//		}
		
		return query;
		
	}
	
	
	/**
	 * 
	 * @return
	 */
	@Override
	protected SelectQuery<Record> group( SelectQuery<Record> query ){
		if( shouldGroup ){
			query.addGroupBy( PrismData.PRISM_DATA.ACTION_ID, PrismData.PRISM_DATA.PLAYER_ID, PrismData.PRISM_DATA.BLOCK_ID, PrismDataExtra.PRISM_DATA_EXTRA.DATA );
			// @todo This isn't support by jooq!
			query.addGroupBy( DSL.field("DATE(FROM_UNIXTIME(epoch))") );
		
		}
		return query;
	}
	
	
	/**
	 * 
	 * @return
	 */
	@Override
	protected SelectQuery<Record> order( SelectQuery<Record> query ){
		String sort_dir = parameters.getSortDirection();
		if( sort_dir.equalsIgnoreCase("asc") ){
			query.addOrderBy( PrismData.PRISM_DATA.EPOCH.asc() );
		} else {
			query.addOrderBy( PrismData.PRISM_DATA.EPOCH.desc() );
		}
		query.addOrderBy( PrismData.PRISM_DATA.X.asc() );
		query.addOrderBy( PrismData.PRISM_DATA.Y.asc() );
		query.addOrderBy( PrismData.PRISM_DATA.Z.asc() );
		if( sort_dir.equalsIgnoreCase("asc") ){
			query.addOrderBy( PrismData.PRISM_DATA.ID.asc() );
		} else {
			query.addOrderBy( PrismData.PRISM_DATA.ID.desc() );
		}
		return query;
	}
	
	
	/**
	 * 
	 * @return
	 */
	@Override
	protected SelectQuery<Record> limit( SelectQuery<Record> query ){
		if( parameters.getProcessType().equals(PrismProcessType.LOOKUP) ){
			int limit = parameters.getLimit();
			if(limit > 0){
				query.addLimit( limit );
			}
		}
		return query;
	}
	

	/**
	 * 
	 * @return
	 */
	protected SelectQuery<Record> buildTimeCondition( SelectQuery<Record> query, Long dateFrom, boolean priorTo ){
		if(dateFrom != null){
			if( !priorTo ){
				query.addConditions( PrismData.PRISM_DATA.EPOCH.ge( UInteger.valueOf( (dateFrom/1000) ) ) );
			} else {
				query.addConditions( PrismData.PRISM_DATA.EPOCH.lt( UInteger.valueOf( (dateFrom/1000) ) ) );
			}
		}
		return query;
	}
}