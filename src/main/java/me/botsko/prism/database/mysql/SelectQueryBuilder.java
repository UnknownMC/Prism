package me.botsko.prism.database.mysql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.bukkit.util.Vector;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.SelectQuery;
import org.jooq.types.UInteger;

import me.botsko.prism.Prism;
import me.botsko.prism.actionlibs.MatchRule;
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
			query.addSelect(PrismData.PRISM_DATA.X.avg());
			query.addSelect(PrismData.PRISM_DATA.Y.avg());
			query.addSelect(PrismData.PRISM_DATA.Z.avg());
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
			query.addSelect(PrismData.PRISM_DATA.ID.count());
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
			PrismData.PRISM_DATA.ID.eq(UInteger.valueOf( id ));
		}
		
		query.addConditions( PrismData.PRISM_DATA.ID.eq(UInteger.valueOf( id )) );
		
		return query;
//		
//		// World conditions
//		if( !parameters.getProcessType().equals(PrismProcessType.DELETE) && parameters.getWorld() != null ){
//			addCondition( String.format( "w.world = '%s'", parameters.getWorld()) );
//		}
//		
//		// Action type
//		HashMap<String,MatchRule> action_types = parameters.getActionTypeNames();
//		// Make sure none of the prism process types are requested
//		boolean containsPrismProcessType = false;
//		boolean hasPositiveMatchRule = false;
//		if( !action_types.isEmpty() ){
//			addCondition( buildMultipleConditions( action_types, "a.action", null ) );
//			for (Entry<String,MatchRule> entry : action_types.entrySet()){
//				if(entry.getKey().contains("prism")){
//					containsPrismProcessType = true;
//					break;
//				}
//				if(entry.getValue().equals(MatchRule.INCLUDE)){
//					hasPositiveMatchRule = true;
//				}
//			}
//		}
//		// exclude internal stuff
//		if( !containsPrismProcessType && !parameters.getProcessType().equals(PrismProcessType.DELETE) && !hasPositiveMatchRule ){
//			addCondition( TBL_DATA + ".action_id NOT IN (69, 70, 71, 72)" );
//		}
//		
//		// Player(s)
//		HashMap<String,MatchRule> playerNames = parameters.getPlayerNames();
//		addCondition( buildMultipleConditions( playerNames, "p.player", null ) );
//		
//		// Radius from loc
//		if( !parameters.getProcessType().equals(PrismProcessType.DELETE) || (parameters.getProcessType().equals(PrismProcessType.DELETE) && parameters.getFoundArgs().containsKey("r") ) ){
//			buildRadiusCondition(parameters.getMinLocation(), parameters.getMaxLocation());
//		}
//		
//		
//		// Blocks
//		HashMap<Integer,Byte> blockfilters = parameters.getBlockFilters();
//		if(!blockfilters.isEmpty()){
//			String[] blockArr = new String[blockfilters.size()];
//			int i = 0;
//			for (Entry<Integer,Byte> entry : blockfilters.entrySet()){
//				if( entry.getValue() == 0 ){
//					blockArr[i] = TBL_DATA+".block_id = " + entry.getKey();
//				} else {
//					blockArr[i] = TBL_DATA+".block_id = " + entry.getKey() + " AND "+TBL_DATA+".block_subid = " +  entry.getValue();
//				}
//				i++;
//			}
//			addCondition( buildGroupConditions(null, blockArr, "%s%s", "OR", null) );
//		}
//		
//		// Entity
//		HashMap<String,MatchRule> entityNames = parameters.getEntities();
//		if( entityNames.size() > 0 ){
//			addCondition( buildMultipleConditions( entityNames, "ex.data", "entity_name\":\"%s" ) );
//		}
//		
//		// Timeframe
//		Long time = parameters.getBeforeTime();
//		if( time != null && time != 0 ){
//			addCondition( buildTimeCondition(time,"<=") );
//		}
//		time = parameters.getSinceTime();
//		if( time != null && time != 0 ){
//			addCondition( buildTimeCondition(time,null) );
//		}
//		
//		// Keyword(s)
//		String keyword = parameters.getKeyword();
//		if(keyword != null){
//			addCondition( "ex.data LIKE '%"+keyword+"%'" );
//		}
//		
//		// Specific coords
//		ArrayList<Location> locations = parameters.getSpecificBlockLocations();
//		if( locations.size() >0 ){
//			String coordCond = "(";
//			int l = 0;
//			for( Location loc : locations ){
//				coordCond += (l > 0 ? " OR" : "" ) + " ("+TBL_DATA+".x = " +(int)loc.getBlockX()+ " AND "+TBL_DATA+".y = " +(int)loc.getBlockY()+ " AND "+TBL_DATA+".z = " +(int)loc.getBlockZ() + ")";
//				l++;
//			}
//			coordCond += ")";
//			addCondition( coordCond );
//		}
//		
//		
//		// Parent process
//		if(parameters.getParentId() > 0){
//			addCondition( String.format("ex.data = %d", parameters.getParentId()) );
//		}
//
//		// Build final condition string
//		int condCount = 1;
//		String query = "";
//		if( conditions.size() > 0 ){
//			for(String cond : conditions){
//				if( condCount == 1 ){
//					query += " WHERE ";
//				}
//				else {
//					query += " AND ";
//				}
//				query += cond;
//				condCount++;
//			}
//		}
		
//		return "";
		
	}
	
	
//	/**
//	 * 
//	 * @return
//	 */
//	protected String group(){
////		if( shouldGroup ){
////			return " GROUP BY "+TBL_DATA+".action_id, "+TBL_DATA+".player_id, "+TBL_DATA+".block_id, ex.data, DATE(FROM_UNIXTIME("+TBL_DATA+".epoch))";
////		}
//		return "";
//	}
	
	
//	/**
//	 * 
//	 * @return
//	 */
//	protected String order(){
//		String sort_dir = parameters.getSortDirection();
//		return " ORDER BY "+TBL_DATA+".epoch "+sort_dir+", x ASC, z ASC, y ASC, id "+sort_dir;	
//	}
	
	
//	/**
//	 * 
//	 * @return
//	 */
//	protected String limit(){
//		if( parameters.getProcessType().equals(PrismProcessType.LOOKUP) ){
//			int limit = parameters.getLimit();
//			if(limit > 0){
//				return " LIMIT "+limit;
//			}
//		}
//		return "";
//	}
	

	
	/**
	 * 
	 * @param origValues
	 * @param field_name
	 * @return
	 */
	protected String buildMultipleConditions( HashMap<String,MatchRule> origValues, String field_name, String format ){
		String query = "";
		if(!origValues.isEmpty()){
			
			ArrayList<String> whereIs = new ArrayList<String>();
			ArrayList<String> whereNot = new ArrayList<String>();
			ArrayList<String> whereIsLike = new ArrayList<String>();
			for (Entry<String,MatchRule> entry : origValues.entrySet()){
				if(entry.getValue().equals(MatchRule.EXCLUDE)){
					whereNot.add(entry.getKey());
				}
				else if(entry.getValue().equals(MatchRule.PARTIAL)){
					whereIsLike.add(entry.getKey());
				} else {
					whereIs.add(entry.getKey());
				}
			}
			// To match
			if(!whereIs.isEmpty()){
				String[] whereValues = new String[whereIs.size()];
				whereValues = whereIs.toArray(whereValues);
				if(format == null){
					query += buildGroupConditions(field_name, whereValues, "%s = '%s'", "OR", null);
				} else {
					query += buildGroupConditions(field_name, whereValues, "%s LIKE '%%%s%%'", "OR", format);
				}
			}
			// To match partial
			if(!whereIsLike.isEmpty()){
				String[] whereValues = new String[whereIsLike.size()];
				whereValues = whereIsLike.toArray(whereValues);
				query += buildGroupConditions(field_name, whereValues, "%s LIKE '%%%s%%'", "OR", format);
			}
			// Not match
			if(!whereNot.isEmpty()){
				String[] whereNotValues = new String[whereNot.size()];
				whereNotValues = whereNot.toArray(whereNotValues);
				
				if(format == null){
					query += buildGroupConditions(field_name, whereNotValues, "%s != '%s'", null, null);
				} else {
					query += buildGroupConditions(field_name, whereNotValues, "%s NOT LIKE '%%%s%%'", null, format);
				}
			}
		}
		return query;
	}
	
	
	/**
	 * 
	 * @param fieldname
	 * @param arg_values
	 * @return
	 */
	protected String buildGroupConditions( String fieldname, String[] arg_values, String matchFormat, String matchType, String dataFormat ){
		
		String where = "";
		matchFormat = (matchFormat == null ? "%s = %s" : matchFormat);
		matchType = (matchType == null ? "AND" : matchType);
		dataFormat = (dataFormat == null ? "%s" : dataFormat);

		if( arg_values.length > 0 && !matchFormat.isEmpty() ){
			where += "(";
			int c = 1;
			for(String val : arg_values){
				if(c > 1 && c <= arg_values.length){
					where += " "+matchType+" ";
				}
				fieldname = ( fieldname == null ? "" : fieldname );
				where += String.format(matchFormat, fieldname, String.format(dataFormat,val));
				c++;
			}
			where += ")";
		}
		return where;
	}
	
	
	/**
	 * 
	 * @param minLoc
     * @param maxLoc
	 * @return
	 */
	protected void buildRadiusCondition( Vector minLoc, Vector maxLoc ){
//		if(minLoc != null && maxLoc != null ){
//			addCondition( "("+TBL_DATA+".x BETWEEN " + minLoc.getBlockX() + " AND " + maxLoc.getBlockX() + ")" );
//			addCondition( "("+TBL_DATA+".y BETWEEN " + minLoc.getBlockY() + " AND " + maxLoc.getBlockY() + ")" );
//			addCondition( "("+TBL_DATA+".z BETWEEN " + minLoc.getBlockZ() + " AND " + maxLoc.getBlockZ() + ")" );
//		}
	}
	
	
//	/**
//	 * 
//	 * @return
//	 */
//	protected String buildTimeCondition( Long dateFrom, String equation ){
//		String where = "";
//		if(dateFrom != null){
//			if(equation == null){
//				addCondition( TBL_DATA+".epoch >= " + (dateFrom/1000) + "" );
//			} else {
//				addCondition( TBL_DATA+".epoch "+equation+" '" + (dateFrom/1000) + "'" );
//			}
//		}
//		return where;
//	}
}