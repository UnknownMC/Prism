package me.botsko.prism.database;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.SelectQuery;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;

import me.botsko.prism.Prism;
import me.botsko.prism.actionlibs.QueryParameters;

public class QueryBuilder {
	
	/**
	 * 
	 */
	protected Prism plugin;
	protected QueryParameters parameters;
	protected boolean shouldGroup;
	
	
	/**
	 * 
	 * @param plugin
	 */
	public QueryBuilder( Prism plugin ){
		this.plugin = plugin;
	}
	
	
	/**
	 * 
	 * @param parameters
	 * @param shouldGroup
	 * @return
	 */
	public String getQuery( QueryParameters parameters, boolean shouldGroup ){
		
		this.parameters = parameters;
		this.shouldGroup = shouldGroup;
		
		// Begin query
		SelectQuery<Record> query = create().selectQuery();
		
		// Ask current handlers to build each segment
		query = select( query );
		query = where( query );
		query = group( query );
		query = order( query );
		query = limit( query );
		

//			if(!parameters.getProcessType().equals(PrismProcessType.DELETE)){
//			}
		

		
		System.out.println(query.getSQL());
		return "SELECT 1;";
		
//		String finalSQL = query.getSQL();
//		if(plugin.getConfig().getBoolean("prism.debug")){
//			Prism.debug(finalSQL);
//		}
		
//		return finalSQL;
		
	}
	
	
	/**
	 * 
	 * @return
	 */
	private DSLContext create(){
		if(plugin.getConfig().getBoolean("prism.debug")){
			return DSL.using( Prism.dbc() , SQLDialect.MYSQL, new Settings().withRenderFormatted(true) );
		}
		return DSL.using( Prism.dbc() , SQLDialect.MYSQL );
	}
	
	
	/**
	 * 
	 */
	protected SelectQuery<Record> select( SelectQuery<Record> query ){
		return query;
	}
	
	
	/**
	 * 
	 */
	protected SelectQuery<Record> where( SelectQuery<Record> query ){
		return query;
	}
	
	
	/**
	 * 
	 */
	protected SelectQuery<Record> group( SelectQuery<Record> query ){
		return query;
	}
	
	
	/**
	 * 
	 */
	protected SelectQuery<Record> order( SelectQuery<Record> query ){
		return query;
	}
	
	
	/**
	 * 
	 */
	protected SelectQuery<Record> limit( SelectQuery<Record> query ){
		return query;
	}
}