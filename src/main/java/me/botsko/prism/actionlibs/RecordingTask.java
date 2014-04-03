package me.botsko.prism.actionlibs;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

import me.botsko.prism.Prism;
import me.botsko.prism.actions.Handler;

public class RecordingTask implements Runnable {

    /**
	 * 
	 */
    private final Prism plugin;

    /**
     * 
     * @param plugin
     */
    public RecordingTask(Prism plugin) {
        this.plugin = plugin;
    }

    /**
	 * 
	 */
    public void save() {
        if( !RecordingQueue.getQueue().isEmpty() ) {
            insertActionsIntoDatabase();
        }
    }

    /**
     * 
     * @param a
     */
    public static int insertActionIntoDatabase(Handler a) {
        int id = 0;
//        Connection conn = null;
//        PreparedStatement s = null;
//        ResultSet generatedKeys = null;
//        try {
//
//            // prepare to save to the db
//            a.save();
//
//
//        } catch ( final SQLException e ) {
//            // plugin.handleDatabaseException( e );
//        } finally {
//            if( generatedKeys != null )
//                try {
//                    generatedKeys.close();
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
     * @throws SQLException
     */
    public void insertActionsIntoDatabase() {

        try {

            int perBatch = plugin.getConfig().getInt( "prism.database.actions-per-insert-batch" );
            if( perBatch < 1 )
                perBatch = 1000;

            if( !RecordingQueue.getQueue().isEmpty() ) {

                Prism.debug( "Beginning batch insert from queue. " + System.currentTimeMillis() );

                // Begin new batch
                List<DBObject> documents = new ArrayList<DBObject>();

                int i = 0;
                while ( !RecordingQueue.getQueue().isEmpty() ) {

                    final Handler a = RecordingQueue.getQueue().poll();

                    // poll() returns null if queue is empty
                    if( a == null )
                        break;

                    if( a.isCanceled() )
                        continue;

                    BasicDBObject doc = new BasicDBObject("world", a.getWorldName()).
                            append("action", a.getType().getName()).
                            append("player", a.getPlayerName()).
                            append("block_id",a.getBlockId()).
                            append("block_subid",a.getBlockSubId()).
                            append("old_block_id",a.getOldBlockId()).
                            append("old_block_subid",a.getOldBlockSubId()).
                            append("x",a.getX()).
                            append("y",a.getY()).
                            append("z",a.getZ()).
                            append("epoch",System.currentTimeMillis() / 1000L).
                            append("data",a.getData());
                    
                    documents.add( doc );
                    
                    // Break out of the loop and just commit what we have
                    if( i >= perBatch ) {
                        Prism.debug( "Recorder: Batch max exceeded, running insert. Queue remaining: "
                                + RecordingQueue.getQueue().size() );
                        break;
                    }
                    i++;
                }
                
                if( documents.isEmpty() ) return;
                
                DBCollection coll = Prism.getMongoCollection();
                WriteResult res = coll.insert( documents );
//                Prism.debug("Recorder logged " + res.getN() + " new actions.");

                // Save the current count to the queue for short historical data
                plugin.queueStats.addRunCount( res.getN() );

            }
        } catch ( final Exception e ) {
            e.printStackTrace();
        }
    }

    /**
	 * 
	 */
    @Override
    public void run() {
        save();
        scheduleNextRecording();
    }

    /**
     * 
     * @return
     */
    protected int getTickDelayForNextBatch() {

        // If we have too many rejected connections, increase the schedule
        if( RecordingManager.failedDbConnectionCount > plugin.getConfig().getInt(
                "prism.database.max-failures-before-wait" ) ) { return RecordingManager.failedDbConnectionCount * 20; }

        int recorder_tick_delay = plugin.getConfig().getInt( "prism.queue-empty-tick-delay" );
        if( recorder_tick_delay < 1 ) {
            recorder_tick_delay = 3;
        }
        return recorder_tick_delay;
    }

    /**
	 * 
	 */
    protected void scheduleNextRecording() {
        if( !plugin.isEnabled() ) {
            Prism.log( "Can't schedule new recording tasks as plugin is now disabled. If you're shutting down the server, ignore me." );
            return;
        }
        plugin.recordingTask = plugin.getServer().getScheduler()
                .runTaskLaterAsynchronously( plugin, new RecordingTask( plugin ), getTickDelayForNextBatch() );
    }
}
