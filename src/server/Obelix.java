package server;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import util.RegistryService;
import base.Athlete;
import base.Event;
import base.EventCategories;
import base.MedalCategories;
import base.NationCategories;
import base.OlympicException;
import base.Results;
import base.Tally;
import client.TabletInterface;

/**
 * Encapsulates the functionality of Obelix.
 * 
 * @author sandeep
 * 
 */
public class Obelix implements ObelixInterface {
	/**
	 * Various data structures forming Obelix's database for the games.
	 */
	private Set<Event> completedEvents;
	private Map<NationCategories, Tally> medalTallies;
	private Map<EventCategories, ArrayList<Athlete>> scores;

	/**
	 * Data structures to manage event subscriptions.
	 */
	private Map<EventCategories, Subscription> subscriptionMap;
	private Map<String, String> subscriberHostMap;

	// To prevent the server from being garbage collected.
	private static Obelix obelixServerInstance;
	private static String orgetorixName = "Orgetorix";

	private static String JAVA_RMI_HOSTNAME_PROPERTY = "java.rmi.server.hostname";
	private static int JAVA_RMI_PORT = 1099;
	private OrgetorixInterface orgetorixStub;

	public Obelix() {
		this.completedEvents = new HashSet<Event>();
		this.medalTallies = new HashMap<NationCategories, Tally>();
		this.scores = new HashMap<EventCategories, ArrayList<Athlete>>();
		this.subscriptionMap = new HashMap<EventCategories, Subscription>();
		this.subscriberHostMap = new HashMap<String, String>();

		for (NationCategories nation : NationCategories.values()) {
			this.medalTallies.put(nation, new Tally());
		}
	}

	private void setupOrgetorixStub(String orgetorixHost)
			throws OlympicException {
		Registry registry = null;
		try {
			registry = LocateRegistry.getRegistry(orgetorixHost, JAVA_RMI_PORT);
			OrgetorixInterface orgetorixStub = (OrgetorixInterface) registry
					.lookup(orgetorixName);
			this.orgetorixStub = orgetorixStub;
		} catch (Exception e) {
			throw new OlympicException("Could not set up Orgetorix Stub.");
		}
	}

	private static Obelix getObelixInstance() {
		if (Obelix.obelixServerInstance == null) {
			Obelix.obelixServerInstance = new Obelix();
		}
		return Obelix.obelixServerInstance;
	}

	/**
	 * Remote method to update results and medal tallies of a completed event.
	 * Called by Cacophonix when it receives an update from Games.
	 */
	public void updateResultsAndTallies(Event simulatedEvent)
			throws RemoteException {
		System.err.println("Received updateResultsAndTallies msg.");
		orgetorixStub.updateResultsAndTallies(simulatedEvent);

	}

	/**
	 * Updates the scores of an on going event. Synchronized as scores are read
	 * to answer client queries.
	 * 
	 * @param eventResult
	 */
	public void updateCurrentScores(EventCategories eventName,
			List<Athlete> currentScores) throws RemoteException {
		System.err.println("Received updateCurrentScores msg.");
		pushCurrentScores(eventName, currentScores);
		orgetorixStub.updateCurrentScores(eventName, currentScores);
	}

	/**
	 * Pushes new scores to all clients subscribed to the event.
	 * 
	 * @param eventName
	 * @param currentScores
	 */
	private void pushCurrentScores(final EventCategories eventName,
			final List<Athlete> currentScores) throws RemoteException {
		System.err.println("Pushing current scores.");
		Thread scoreThread = new Thread(new Runnable() {

			@Override
			public void run() {
				sendScoresToSubscribers(eventName, currentScores);
			}
		}, "Score Update Thread");
		scoreThread.start();
	}

	/**
	 * Pushes final results of an event to all it's subscribers.
	 * 
	 * @param completedEvent
	 */
	private void pushResults(final Event completedEvent) {
		System.err.println("Pushing results.");
		Thread resultThread = new Thread(new Runnable() {

			@Override
			public void run() {
				sendResultsToSubscribers(completedEvent.getName(),
						completedEvent.getResult());
			}
		}, "Result Update Thread");
		resultThread.start();
	}

	/**
	 * Remote function that can be called by clients to get the results of a
	 * completed event.
	 */
	public Results getResults(EventCategories eventName) {
		System.err.println("Sending results for " + eventName + ".");
		try {
			Results result = orgetorixStub.getResults(eventName);
			return result;
		} catch (RemoteException r) {
			return null;
		}

	}

	/**
	 * Remote function that can be called by clients to get the current scores
	 * of an on going event.
	 */
	public List<Athlete> getCurrentScores(EventCategories eventName)
			throws RemoteException {
		System.err.println("Sending current scores for " + eventName + ".");
		try {
			return orgetorixStub.getCurrentScores(eventName);
		} catch (RemoteException r) {
			return null;

		}
	}

	/**
	 * Remote function that can be called by clients to get the medal tally of a
	 * particular team.
	 */
	public Tally getMedalTally(NationCategories teamName) {
		System.err.println("Sending medal tally for " + teamName + ".");
		try{
			return orgetorixStub.getMedalTally(teamName);
		}catch(RemoteException r){
			return null;
		}
	}

	/**
	 * Remote function that can be called by a client to create a subscription
	 * to a particular event.
	 */
	public void registerClient(String clientID, String clientHost,
			EventCategories eventName) {
		System.err.println("Registering client " + clientID + ".");
		Subscription subscription = null;

		synchronized (this.subscriptionMap) {
			if (this.subscriptionMap.containsKey(eventName)) {
				subscription = this.subscriptionMap.get(eventName);
			} else {
				subscription = new Subscription();
				subscription.setEventName(eventName);
				this.subscriptionMap.put(eventName, subscription);
			}

			subscription.addSubscriber(clientID);
		}

		synchronized (this.subscriberHostMap) {
			this.subscriberHostMap.put(clientID, clientHost);
		}

		for (Event completedEvent : completedEvents) {
			if (completedEvent.getName() == eventName) {
				pushResults(completedEvent);
				return;
			}
		}
	}

	/**
	 * Pushes new scores of an event to all subscribers of that event. Also
	 * measures the average push latency across all subscribers for each update
	 * set.
	 * 
	 * @param eventName
	 * @param currentScores
	 * @throws NotBoundException
	 * @throws RemoteException
	 */
	private void sendScoresToSubscribers(EventCategories eventName,
			List<Athlete> currentScores) {
		Subscription subscription = null;

		synchronized (this.subscriptionMap) {
			subscription = this.subscriptionMap.get(eventName);
		}

		if (subscription == null) {
			return;
		}

		long startTime = System.currentTimeMillis();
		synchronized (this.subscriptionMap) {
			for (String subscriber : subscription.getSubscribers()) {
				TabletInterface tabletStub;
				try {
					tabletStub = setupObelixClient(subscriber);
					tabletStub.updateScores(eventName, currentScores);
				} catch (NotBoundException e) {
					e.printStackTrace();
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}
		}
		long duration = System.currentTimeMillis() - startTime;
		System.out.println("Average push latency: "
				+ (duration / subscription.getSubscribers().size()));
	}

	/**
	 * Helper function to setup Obelix client that is used to push score updates
	 * and results to subscribers.
	 * 
	 * @param subscriber
	 * @return TabletInterface
	 * @throws RemoteException
	 * @throws NotBoundException
	 */
	private TabletInterface setupObelixClient(String subscriber)
			throws RemoteException, NotBoundException {
		Registry registry = null;
		synchronized (this.subscriberHostMap) {
			registry = LocateRegistry.getRegistry(
					this.subscriberHostMap.get(subscriber), JAVA_RMI_PORT);
			TabletInterface tabletStub = (TabletInterface) registry
					.lookup(subscriber);
			return tabletStub;
		}
	}

	/**
	 * Sends final results of an event to all subscribers of that event.
	 * 
	 * @param eventName
	 * @param result
	 */
	private void sendResultsToSubscribers(EventCategories eventName,
			Results result) {
		Registry registry = null;
		Subscription subscription = null;

		synchronized (this.subscriptionMap) {
			subscription = this.subscriptionMap.remove(eventName);
		}

		if (subscription == null) {
			return;
		}

		synchronized (this.subscriberHostMap) {
			for (String subscriber : subscription.getSubscribers()) {
				TabletInterface tabletStub;
				try {
					tabletStub = setupObelixClient(subscriber);
					tabletStub.updateResults(eventName, result);
				} catch (RemoteException e) {
					e.printStackTrace();
				} catch (NotBoundException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Helper function to setup the server at Obelix.
	 * 
	 * @param regService
	 * @throws IOException
	 * @throws OlympicException
	 */
	private void setupObelixServer(RegistryService regService)
			throws IOException, OlympicException {
		Registry registry = null;
		String SERVER_NAME = "Obelix";

		ObelixInterface serverStub = (ObelixInterface) UnicastRemoteObject
				.exportObject(Obelix.getObelixInstance(), 0);
		try {
			registry = LocateRegistry.getRegistry(JAVA_RMI_PORT);
			registry.rebind(SERVER_NAME, serverStub);
			System.err.println("Registry Service running at "
					+ regService.getLocalIPAddress() + ".");
			System.err.println("Obelix ready.");
		} catch (RemoteException e) {
			regService.setupLocalRegistry();
			registry = LocateRegistry.getRegistry(JAVA_RMI_PORT);
			registry.rebind(SERVER_NAME, serverStub);
			System.err.println("New Registry Service created. Obelix ready.");
		}
	}

	/**
	 * Sets up Obelix's client and server stubs so it may perform it's function
	 * of servicing client requests and registering updates from Cacophonix.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws OlympicException {

		// Bind the remote object's stub in the registry
		Obelix obelixInstance = Obelix.getObelixInstance();
		String orgetorixHost = args[0];
		try {
			RegistryService regService = new RegistryService();
			System.setProperty(JAVA_RMI_HOSTNAME_PROPERTY,
					regService.getLocalIPAddress());
			obelixInstance.setupObelixServer(regService);
			obelixInstance.setupOrgetorixStub(orgetorixHost);
		} catch (IOException e) {
			throw new OlympicException(
					"Registry Service could not be created.", e);
		}
	}
}