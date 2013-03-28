package me.botsko.prism.commands;

import org.bukkit.ChatColor;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.PlayerInventory;

import me.botsko.prism.Prism;
import me.botsko.prism.commandlibs.CallInfo;
import me.botsko.prism.commandlibs.SubHandler;
import me.botsko.prism.settings.Settings;
import me.botsko.prism.utils.ItemUtils;
import me.botsko.prism.wands.InspectorWand;
import me.botsko.prism.wands.ProfileWand;
import me.botsko.prism.wands.RestoreWand;
import me.botsko.prism.wands.RollbackWand;
import me.botsko.prism.wands.Wand;

public class WandCommand implements SubHandler {
	
	/**
	 * 
	 */
	private Prism plugin;
	
	
	/**
	 * 
	 * @param plugin
	 * @return 
	 */
	public WandCommand(Prism plugin) {
		this.plugin = plugin;
	}
	
	
	/**
	 * Handle the command
	 */
	public void handle(CallInfo call) {
		
		String type = "i";
		if(call.getArgs().length == 2){
			type = call.getArg(1);
		}
		
		Wand oldwand = null;
		if(plugin.playersWithActiveTools.containsKey(call.getPlayer().getName())){
			// Pull the wand in use
			oldwand = plugin.playersWithActiveTools.get(call.getPlayer().getName());
		}
		
		// Always remove the old one
		plugin.playersWithActiveTools.remove(call.getPlayer().getName());
		
		
		// Determine default mode
		String mode = plugin.getConfig().getString("prism.wands.default-mode");
		
		// Check if the player has a personal override
		if( plugin.getConfig().getBoolean("prism.wands.allow-user-override") ){
			String personalMode = Settings.getSetting("wand.mode", call.getPlayer());
			if( personalMode != null ){
				mode = personalMode;
			}
		}
		
		// Determine which item we're using.
		int item_id = 0;
		byte item_subid = -1;
		String toolKey = null;
		if( mode.equals("item") ){
			toolKey = plugin.getConfig().getString("prism.wands.default-item-mode-id");
		}
		else if( mode.equals("block") ){
			toolKey = plugin.getConfig().getString("prism.wands.default-block-mode-id");
		}
		
		// Check if the player has a personal override
		if( plugin.getConfig().getBoolean("prism.wands.allow-user-override") ){
			String personalToolKey = Settings.getSetting("wand.item", call.getPlayer());
			if( personalToolKey != null ){
				toolKey = personalToolKey;
			}
		}
		
		if( toolKey != null ){
			if(!toolKey.contains(":")){
				toolKey += ":0";
			}
			String[] toolKeys = toolKey.split(":");
			item_id = Integer.parseInt(toolKeys[0]);
			item_subid = Byte.parseByte(toolKeys[1]);
		}
		
		String wandOn = "";
		String item_name = "";
		if( item_id != 0 ){
			item_name = plugin.getItems().getItemStackAliasById(item_id, item_subid);
			wandOn += " on a " + item_name;
		}
		
		if( !ItemUtils.isAcceptableWand( item_id, item_subid ) ){
			call.getPlayer().sendMessage( Prism.messenger.playerError("Sorry but you may not use " + item_name + " for a wand.") );
			return;
		}
		
		boolean enabled = false;
		Wand wand = null;
			
		/**
		 * Inspector wand
		 */
		if( type.equalsIgnoreCase("i") || type.equalsIgnoreCase("inspect") ){
			if( !call.getPlayer().hasPermission("prism.lookup") && !call.getPlayer().hasPermission("prism.wand.inspect") ){
				call.getPlayer().sendMessage( Prism.messenger.playerError("You do not have permission for this.") );
				return;
			}
			if(oldwand != null && oldwand instanceof InspectorWand){
				call.getPlayer().sendMessage( Prism.messenger.playerHeaderMsg("Inspection wand " + ChatColor.RED + "disabled"+ChatColor.WHITE+".") );
			} else {
				wand = new InspectorWand( plugin );
				call.getPlayer().sendMessage( Prism.messenger.playerHeaderMsg("Inspection wand " + ChatColor.GREEN + "enabled"+ChatColor.WHITE+wandOn+".") );
				enabled = true;
			}
		}
		
		/**
		 * Profile wand
		 */
		else if( type.equalsIgnoreCase("p") || type.equalsIgnoreCase("profile") ){
			if( !call.getPlayer().hasPermission("prism.lookup") && !call.getPlayer().hasPermission("prism.wand.profile") ){
				call.getPlayer().sendMessage( Prism.messenger.playerError("You do not have permission for this.") );
				return;
			}
			if( oldwand != null && oldwand instanceof ProfileWand ){
				call.getPlayer().sendMessage( Prism.messenger.playerHeaderMsg("Profile wand " + ChatColor.RED + "disabled"+ChatColor.WHITE+".") );
			} else {
				wand = new ProfileWand( plugin );
				call.getPlayer().sendMessage( Prism.messenger.playerHeaderMsg("Profile wand " + ChatColor.GREEN + "enabled"+ChatColor.WHITE+wandOn+".") );
				enabled = true;
			}
		}

		
		/**
		 * Rollback wand
		 */
		else if( type.equalsIgnoreCase("rollback") ){
			if( !call.getPlayer().hasPermission("prism.rollback") && !call.getPlayer().hasPermission("prism.wand.rollback") ){
				call.getPlayer().sendMessage( Prism.messenger.playerError("You do not have permission for this.") );
				return;
			}
			if(oldwand != null && oldwand instanceof RollbackWand){
				call.getPlayer().sendMessage( Prism.messenger.playerHeaderMsg("Rollback wand " + ChatColor.RED + "disabled"+ChatColor.WHITE+".") );
			} else {
				wand = new RollbackWand( plugin );
				call.getPlayer().sendMessage( Prism.messenger.playerHeaderMsg("Rollback wand " + ChatColor.GREEN + "enabled"+ChatColor.WHITE+wandOn+".") );
				enabled = true;
			}
		}
		
		/**
		 * Restore wand
		 */
		else if(type.equalsIgnoreCase("restore")){
			if( !call.getPlayer().hasPermission("prism.restore") && !call.getPlayer().hasPermission("prism.wand.restore") ){
				call.getPlayer().sendMessage( Prism.messenger.playerError("You do not have permission for this.") );
				return;
			}
			// If disabling this one
			if(oldwand != null && oldwand instanceof RestoreWand){
				call.getPlayer().sendMessage( Prism.messenger.playerHeaderMsg("Restore wand " + ChatColor.RED + "disabled"+ChatColor.WHITE+".") );
			} else {
				wand = new RestoreWand( plugin );
				call.getPlayer().sendMessage( Prism.messenger.playerHeaderMsg("Restore wand " + ChatColor.GREEN + "enabled"+ChatColor.WHITE+wandOn+".") );
				enabled = true;
			}
		}
		
		/**
		 * Off
		 */
		else if(type.equalsIgnoreCase("off")){
			call.getPlayer().sendMessage( Prism.messenger.playerHeaderMsg("Current wand " + ChatColor.RED + "disabled"+ChatColor.WHITE+".") );
		}
		
		// Not a valid wand
		else {
			call.getPlayer().sendMessage( Prism.messenger.playerError("Invalid wand type. Use /prism ? for help.") );
			return;
		}
		
		PlayerInventory inv = call.getPlayer().getInventory();
		if( enabled ){
			
			wand.setWandMode(mode);
			wand.setItemId(item_id);
			wand.setItemSubId(item_subid);
			
			Prism.debug("Wand activated for player - mode: " + mode + " Item:" + item_id + ":" + item_subid);
			
			// Move any existing item to the hand, otherwise give it to them
			if( plugin.getConfig().getBoolean("prism.wands.auto-equip") ){
				if( !ItemUtils.moveItemToHand( inv, item_id, item_subid) ){
					// Store the item they're holding, if any
					wand.setOriginallyHeldItem( inv.getItemInHand() );
					// They don't have the item, so we need to give them an item
					if( ItemUtils.handItemToPlayer( inv,  new ItemStack(item_id,1,item_subid) ) ){
						wand.setItemWasGiven(true);
					} else {
						call.getPlayer().sendMessage( Prism.messenger.playerError("Can't fit the wand item into your inventory.") );
					}
				}
				call.getPlayer().updateInventory();
			}
			// Store
			plugin.playersWithActiveTools.put(call.getPlayer().getName(), wand);
		} else {
			if(oldwand != null){
				oldwand.disable( call.getPlayer() );
			}
		}
	}
}