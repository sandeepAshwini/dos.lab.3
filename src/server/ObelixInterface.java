package server;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import base.Athlete;
import base.Event;
import base.EventCategories;
import base.NationCategories;
import base.Results;
import base.Tally;

/**
 * Declares the functions exported by Obelix.
 * 
 * @author aravind
 * 
 */
public interface ObelixInterface extends Remote {
	public void updateResultsAndTallies(Event simulatedEvent)
			throws RemoteException;

	public void updateCurrentScores(EventCategories eventType,
			List<Athlete> currentScores) throws RemoteException;

	public Tally getMedalTally(NationCategories teamName, String clientID)
			throws RemoteException;

	public Results getResults(EventCategories eventName, String clientID)
			throws RemoteException;

	public List<Athlete> getCurrentScores(EventCategories eventName,
			String clientID) throws RemoteException;

	public void registerClient(String clientID, String clientHost,
			EventCategories eventName) throws RemoteException;

	//public String conductLottery() throws RemoteException;

	//public String getLotteryWinner(String clientID) throws RemoteException;

	public void notifyScoreCaching(String serverID, EventCategories eventName)
			throws RemoteException;

	public void notifyResultCaching(String serverID, EventCategories eventName)
			throws RemoteException;

	public void notifyTallyCaching(String serverID, NationCategories nation)
			throws RemoteException;

	public void invalidateScores(EventCategories eventName)
			throws RemoteException;

	public void invalidateResults(EventCategories eventName)
			throws RemoteException;

	public void invalidateTallies(NationCategories nation)
			throws RemoteException;

	public void notifyMaster() throws RemoteException;

	public void setMaster(String masterName) throws RemoteException;

}
