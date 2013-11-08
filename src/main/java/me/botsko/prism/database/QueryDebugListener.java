package me.botsko.prism.database;

import me.botsko.prism.Prism;

import org.jooq.ExecuteContext;
import org.jooq.conf.ParamType;
import org.jooq.impl.DefaultExecuteListener;

public class QueryDebugListener extends DefaultExecuteListener {

	private static final long serialVersionUID = 3817683055059014043L;

	
	/**
	 * 
	 */
    @Override
    public void renderEnd(ExecuteContext ctx) {
    	Prism.debug(ctx.query().getSQL(ParamType.INLINED));
    }
}
