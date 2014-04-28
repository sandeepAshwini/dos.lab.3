package server;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import base.Athlete;
import base.Event;

/**
 * Declares the functions exported by Cacophonix.
 * 
 * @author sandeep
 * 
 */
public interface CacophonixInterface extends Remote {
	public void updateResultsAndTallies(Event simulatedEvent)
			throws RemoteException;

	public void updateCurrentScores(Event simulatedEvent,
			List<Athlete> currentScores) throws RemoteException;

	//public String conductLottery() throws RemoteException;
}
