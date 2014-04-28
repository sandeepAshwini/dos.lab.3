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

public interface OrgetorixInterface extends Remote {
	public void updateResultsAndTallies(Event simulatedEvent)
			throws RemoteException;

	public void updateCurrentScores(EventCategories eventType,
			List<Athlete> currentScores) throws RemoteException;

	public Tally getMedalTally(NationCategories teamName)
			throws RemoteException;

	public Results getResults(EventCategories eventName) throws RemoteException;

	public List<Athlete> getCurrentScores(EventCategories eventName)
			throws RemoteException;

}
