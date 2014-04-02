package me.botsko.prism.actionlibs;

import org.bukkit.Bukkit;
import org.bukkit.scheduler.BukkitScheduler;

import com.mongodb.MongoException;

import me.botsko.prism.Prism;

public class InternalAffairs implements Runnable {

    private final Prism plugin;

    /**
     * 
     * @param prism
     */
    public InternalAffairs(Prism plugin) {
        Prism.debug( "[InternalAffairs] Keeping watch over the watchers." );
        this.plugin = plugin;
    }

    /**
	 * 
	 */
    @Override
    public void run() {

        if( plugin.recordingTask != null ) {

            final int taskId = plugin.recordingTask.getTaskId();

            final BukkitScheduler scheduler = Bukkit.getScheduler();

            // is recording task running?
            if( scheduler.isCurrentlyRunning( taskId ) || scheduler.isQueued( taskId ) ) {
                Prism.debug( "[InternalAffairs] Recorder is currently active. All is good." );
                return;
            }
        }

        Prism.log( "[InternalAffairs] Recorder is NOT active... checking database" );

        // is db connection valid?
        try {
            if( Prism.getMongo() == null ) {
                Prism.log( "[InternalAffairs] Pool returned NULL instead of a valid connection." );
                return;
            }
            Prism.getMongo().getDB("prism");
        } catch ( final MongoException e ) {
            Prism.debug( "[InternalAffairs] Error: " + e.getMessage() );
            e.printStackTrace();
        }
    }
}