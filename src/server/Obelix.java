package server;

import java.io.IOException;
import java.net.SocketException;
import java.rmi.ConnectException;
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

import util.BullyElectedBerkeleySynchronized;
import util.LamportClock;
import util.Lottery;
import util.LotteryManager;
import util.RegistryService;
import util.ServerDetail;
import util.ServiceComponent;
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
/**
 * @author aravind
 * 
 */
public class Obelix extends ServiceComponent implements ObelixInterface {

	/**
	 * Various data structures forming Obelix's database for the games.
	 */
	private Set<Event> completedEvents;

	private ScoreCache scoreCache;
	private ResultCache resultCache;
	private TallyCache tallyCache;

	/**
	 * Data structures to manage event subscriptions.private
	 * Map<EventCategories, ArrayList<Athlete>> scores;
	 */
	private Map<EventCategories, Subscription> subscriptionMap;
	private Map<String, String> subscriberHostMap;

	// To prevent the server from being garbage collected.
	private static Obelix obelixServerInstance;
	private static String OBELIX_SERVICE_NAME = "Obelix";
	private static String ORGETORIX_SERVICE_NAME = "Orgetorix";
	private static String JAVA_RMI_HOSTNAME_PROPERTY = "java.rmi.server.hostname";
	private static String SERVICE_FINDER_HOST;
	private static int SERVICE_FINDER_PORT;
	private static boolean MASTER_PUSH;
	private static int RETRY_LIMIT = 3;
	private static int RETRY_WAIT = 3000;

	private OrgetorixInterface orgetorixStub;
	// private Lottery lottery = new Lottery();
	// private boolean lotteryFrozen;
	// private Integer localRequestCounter = 0;
	// private String lotteryWinner;

	private Map<String, Set<EventCategories>> scoreCacherList;
	private Map<String, Set<EventCategories>> resultCacherList;
	private Map<String, Set<NationCategories>> tallyCacherList;

	public Obelix(String serviceFinderHost, int serviceFinderPort) {
		super(OBELIX_SERVICE_NAME, serviceFinderHost, serviceFinderPort);
		this.completedEvents = new HashSet<Event>();
		this.subscriptionMap = new HashMap<EventCategories, Subscription>();
		this.subscriberHostMap = new HashMap<String, String>();
		// this.lotteryFrozen = false;
		// this.lotteryWinner = null;
		this.scoreCacherList = new HashMap<String, Set<EventCategories>>();
		this.resultCacherList = new HashMap<String, Set<EventCategories>>();
		this.tallyCacherList = new HashMap<String, Set<NationCategories>>();

		this.tallyCache = new TallyCache();
		this.scoreCache = new ScoreCache();
		this.resultCache = new ResultCache();
	}

	/**
	 * Sets up the Orgetorix (backend process) client stub by looking up the
	 * address using {@link ServiceFinder}
	 * 
	 * @throws OlympicException
	 */
	private void setupOrgetorixStub() throws OlympicException {
		Registry registry = null;
		try {
			ServerDetail orgetorixDetail = this
					.getServerDetails(ORGETORIX_SERVICE_NAME);
			registry = LocateRegistry.getRegistry(
					orgetorixDetail.getServiceAddress(),
					orgetorixDetail.getServicePort());
			OrgetorixInterface orgetorixStub = (OrgetorixInterface) registry
					.lookup(orgetorixDetail.getServerName());
			this.orgetorixStub = orgetorixStub;
		} catch (Exception e) {
			throw new OlympicException("Could not set up Orgetorix Stub.");
		}
	}

	private ObelixInterface getObelixMasterStub() throws RemoteException,
			NotBoundException {
		Registry registry = null;
		ObelixInterface obelixMasterStub = null;

		for (int i = 0; i < RETRY_LIMIT; i++) {
			try {
				ServerDetail obelixMasterDetail = this.getServerDetails(
						OBELIX_SERVICE_NAME, this.getServerName());
				registry = LocateRegistry.getRegistry(
						obelixMasterDetail.getServiceAddress(),
						obelixMasterDetail.getServicePort());
				obelixMasterStub = (ObelixInterface) registry
						.lookup(obelixMasterDetail.getServerName());
			} catch (ConnectException | NotBoundException e) {
				if (i >= RETRY_LIMIT) {
					throw e;
				}
				try {
					Thread.sleep(RETRY_WAIT);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}

		return obelixMasterStub;
	}

	private ObelixInterface getObelixSlaveStub(String serverName)
			throws RemoteException, ServerNotFoundException {
		Registry registry = null;
		ObelixInterface obelixSlaveStub = null;
		try {
			ServerDetail obelixSlaveDetail = this
					.getSpecificServerDetails(serverName);
			if (obelixSlaveDetail == null) {
				throw new ServerNotFoundException("Obelix slave not found.");
			}
			registry = LocateRegistry.getRegistry(
					obelixSlaveDetail.getServiceAddress(),
					obelixSlaveDetail.getServicePort());
			obelixSlaveStub = (ObelixInterface) registry
					.lookup(obelixSlaveDetail.getServerName());
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		return obelixSlaveStub;
	}

	private ObelixInterface getObelixSlaveStub(ServerDetail serverDetail)
			throws RemoteException, ServerNotFoundException {
		Registry registry = null;
		ObelixInterface obelixSlaveStub = null;
		try {
			registry = LocateRegistry.getRegistry(
					serverDetail.getServiceAddress(),
					serverDetail.getServicePort());
			obelixSlaveStub = (ObelixInterface) registry.lookup(serverDetail
					.getServerName());
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		return obelixSlaveStub;
	}

	private static Obelix getObelixInstance() {
		if (obelixServerInstance == null) {
			obelixServerInstance = new Obelix(SERVICE_FINDER_HOST,
					SERVICE_FINDER_PORT);
		}
		return obelixServerInstance;
	}

	private void setupHeartbeatThread() {
		Thread heartbeatThread = new Thread(new HeartbeatNotifier(this),
				"HeartbeatThread");
		heartbeatThread.start();
	}

	/**
	 * Remote method to update results and medal tallies of a completed event.
	 * Called by Cacophonix when it receives an update from Games.
	 */
	public void updateResultsAndTallies(Event simulatedEvent)
			throws RemoteException {
		System.err.println("Received updateResultsAndTallies msg.");
		if (MASTER_PUSH == true) {
			System.err.println("Invalidating results and tallies in caches.");
			this.cleanUpResultCaches(simulatedEvent.getName());
			for (MedalCategories medalType : MedalCategories.values()) {
				this.cleanUpTallyCaches(simulatedEvent.getResult().getTeam(
						medalType));
			}
		}
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
		if (MASTER_PUSH == true) {
			System.err.println("Invalidating scores in caches.");
			this.cleanUpScoreCaches(eventName);
		}
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
	public Results getResults(EventCategories eventName, String clientID) {
		try {
			// this.notifyEvent(clientID);
			try {
				Results result = null;
				if (MASTER_PUSH == false) {
					result = this.resultCache.getResults(eventName,
							System.currentTimeMillis());
				} else {
					result = this.resultCache.getResults(eventName);
				}
				System.out.println("Sending results for " + eventName
						+ " from cache.");
				return result;
			} catch (OlympicException o) {
				if (MASTER_PUSH == true) {
					ObelixInterface masterStub = this.getObelixMasterStub();
					masterStub.notifyResultCaching(this.getServerName(),
							eventName);
				}

				Results result = orgetorixStub.getResults(eventName);

				System.out.println("Sending results for " + eventName
						+ " from database.");

				if (result == null) {
					return null;
				}
				if (MASTER_PUSH == false) {
					this.resultCache.cache(eventName, result,
							System.currentTimeMillis());
				} else {
					this.resultCache.cache(eventName, result);

				}

				return result;
			}
		} catch (RemoteException | NotBoundException e) {
			return null;
		}

	}

	/**
	 * Remote function that can be called by clients to get the current scores
	 * of an on going event.
	 */
	public List<Athlete> getCurrentScores(EventCategories eventName,
			String clientID) throws RemoteException {
		try {
			// this.notifyEvent(clientID);
			try {
				List<Athlete> scores = null;
				if (MASTER_PUSH == false) {
					scores = this.scoreCache.getScores(eventName,
							System.currentTimeMillis());
				} else {
					scores = this.scoreCache.getScores(eventName);
				}
				System.out.println("Sending current scores for " + eventName
						+ " from cache.");
				return scores;
			} catch (OlympicException o) {
				if (MASTER_PUSH == true) {
					ObelixInterface masterStub = this.getObelixMasterStub();
					masterStub.notifyScoreCaching(this.getServerName(),
							eventName);
				}
				List<Athlete> scores = orgetorixStub
						.getCurrentScores(eventName);

				System.out.println("Sending current scores for " + eventName
						+ " from database.");
				if (scores == null) {
					return null;
				}
				if (MASTER_PUSH == false) {
					this.scoreCache.cache(eventName, scores,
							System.currentTimeMillis());
				} else {
					this.scoreCache.cache(eventName, scores);

				}
				return scores;
			}
		} catch (RemoteException | NotBoundException e) {
			return null;

		}
	}

	/**
	 * Remote function that can be called by clients to get the medal tally of a
	 * particular team.
	 */
	public Tally getMedalTally(NationCategories teamName, String clientID) {
		try {
			// this.notifyEvent(clientID);
			try {
				Tally medalTally = null;
				if (MASTER_PUSH == false) {
					medalTally = this.tallyCache.getTally(teamName,
							System.currentTimeMillis());
				} else {
					medalTally = this.tallyCache.getTally(teamName);
				}
				System.out.println("Sending medal tally for " + teamName
						+ " from cache.");
				return medalTally;
			} catch (OlympicException o) {
				if (MASTER_PUSH == true) {
					ObelixInterface masterStub = this.getObelixMasterStub();
					masterStub.notifyTallyCaching(this.getServerName(),
							teamName);
				}
				Tally medalTally = orgetorixStub.getMedalTally(teamName);
				System.out.println("Sending medal tally for " + teamName
						+ " from database.");
				if (medalTally == null) {
					return null;
				}
				if (MASTER_PUSH == false) {
					this.tallyCache.cache(teamName, medalTally,
							System.currentTimeMillis());
				} else {
					this.tallyCache.cache(teamName, medalTally);
				}

				return medalTally;
			}
		} catch (RemoteException | NotBoundException e) {
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
		// TODO: REMOVE SUBSCRIBER HOSTMAP AND SIMPLY USE SERVICEFINDER HERE.
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
	 * Helper function to setup the server at Obelix and register with
	 * {@link ServiceFinder}.
	 * 
	 * @param regService
	 * @throws IOException
	 * @throws OlympicException
	 */
	private void setupObelixServer(RegistryService regService)
			throws IOException, OlympicException {
		Registry registry = null;

		// this.register(OBELIX_SERVICE_NAME, regService.getLocalIPAddress(),
		// JAVA_RMI_PORT);
		ObelixInterface serverStub = (ObelixInterface) UnicastRemoteObject
				.exportObject(Obelix.getObelixInstance(), 0);
		try {
			registry = LocateRegistry.getRegistry(JAVA_RMI_PORT);
			registry.rebind(this.getServerName(), serverStub);
			this.register(OBELIX_SERVICE_NAME, regService.getLocalIPAddress(),
					JAVA_RMI_PORT);
			System.err.println("Registry Service running at "
					+ regService.getLocalIPAddress() + ":" + JAVA_RMI_PORT
					+ ".");
			System.err.println("Obelix ready.");
		} catch (RemoteException e) {
			registry = regService.setupLocalRegistry(JAVA_RMI_PORT);
			registry.rebind(this.getServerName(), serverStub);
			this.register(OBELIX_SERVICE_NAME, regService.getLocalIPAddress(),
					JAVA_RMI_PORT);
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
		if (args[0].compareTo("--masterpush") == 0) {
			MASTER_PUSH = true;
		} else if (args[0].compareTo("--proxypull") == 0) {
			MASTER_PUSH = false;
		} else {
			usage();
			System.exit(-1);
		}

		if (args.length < 2) {
			usage();
			System.exit(-1);
		} else {
			SERVICE_FINDER_HOST = args[1];
		}

		SERVICE_FINDER_PORT = (args.length < 3) ? DEFAULT_JAVA_RMI_PORT
				: Integer.parseInt(args[2]);

		JAVA_RMI_PORT = (args.length < 4) ? DEFAULT_JAVA_RMI_PORT : Integer
				.parseInt(args[3]);

		final Obelix obelixInstance = Obelix.getObelixInstance();
		try {
			RegistryService regService = new RegistryService();
			System.setProperty(JAVA_RMI_HOSTNAME_PROPERTY,
					regService.getLocalIPAddress());
			obelixInstance.setupObelixServer(regService);
			obelixInstance.setupHeartbeatThread();
			obelixInstance.setupOrgetorixStub();
			// obelixInstance.initiateElection();
		} catch (IOException e) {
			throw new OlympicException(
					"Registry Service could not be created.", e);
		}

	}

	private static void usage() {
		System.out
				.println("java -cp ./bin/ -Djava.rmi.server.codebase=file:./bin/"
						+ " server.Obelix <insert host address displayed by ServiceFinder>"
						+ " <insert port number displayed by ServiceFinder> [RMI_PORT]");
	}

	// /**
	// * Sets up a client stub of type BullyElectableFrontend.
	// *
	// * @param participant
	// * @throws RemoteException
	// */
	// public LotteryManager getLotteryManagerClientStub(ServerDetail
	// participant)
	// throws RemoteException {
	// Registry registry = null;
	// LotteryManager client = null;
	// registry = LocateRegistry.getRegistry(participant.getServiceAddress(),
	// participant.getServicePort());
	// try {
	// client = (LotteryManager) registry.lookup(participant
	// .getServerName());
	// } catch (NotBoundException e) {
	// e.printStackTrace();
	// }
	// return client;
	// }
	//
	// /**
	// * Notifies the occurrence of a new event by synchronizing current process
	// * timestamp with other processes. Each new request received counts as a
	// new
	// * event.
	// *
	// * @param participantID
	// * @throws RemoteException
	// */
	// private void notifyEvent(String participantID) throws RemoteException {
	// long timestampValue = this.syncServers();
	// if (!lotteryFrozen) {
	// synchronized (this.localRequestCounter) {
	// localRequestCounter++;
	// }
	// if (timestampValue % lottery.lotteryEnterFrequency == 0) {
	// System.out.println("Entering " + participantID
	// + " into lottery.");
	// this.addParticipant(participantID);
	// syncParticipants(participantID);
	// }
	// }
	// }
	//
	// private void syncParticipants(String participantID) throws
	// RemoteException {
	// List<ServerDetail> participants =
	// findAllParticipants(OBELIX_SERVICE_NAME);
	// for (ServerDetail participant : participants) {
	// if (participant.getPID() == this.PID) {
	// continue;
	// }
	//
	// LotteryManager clientStub = getLotteryManagerClientStub(participant);
	// clientStub.addParticipant(participantID);
	// }
	// }
	//
	// public void addParticipant(String participantID) throws RemoteException {
	// synchronized (this.lottery) {
	// this.lottery.addParticipant(participantID);
	// }
	// }
	//
	// /**
	// * Implements totally-ordered multicasting. Multicasts current process'
	// * timestamp and waits for updated timestamps from all processes.
	// *
	// * @return Update timestamp for current process
	// * @throws RemoteException
	// */
	//
	// private synchronized long syncServers() throws RemoteException {
	// this.timeStamp.tick();
	// List<ServerDetail> participants =
	// findAllParticipants(OBELIX_SERVICE_NAME);
	// List<LamportClock> lamportClocks = new ArrayList<LamportClock>();
	// for (ServerDetail participant : participants) {
	// if (participant.getPID() == this.PID) {
	// continue;
	// }
	// LotteryManager clientStub = getLotteryManagerClientStub(participant);
	// lamportClocks.add(clientStub.notifyTimeStamp(this.timeStamp));
	// }
	// for (LamportClock incomingClock : lamportClocks) {
	// this.timeStamp.synchronizeTime(incomingClock);
	// }
	//
	// return this.timeStamp.getTime();
	// }
	//
	// /**
	// * Utility function that initiates the lottery draw.
	// *
	// * @return clientID of the winner of the lottery.
	// * @throws RemoteException
	// */
	// @Override
	// public String conductLottery() throws RemoteException {
	// List<ServerDetail> participants =
	// findAllParticipants(OBELIX_SERVICE_NAME);
	// List<LotteryManager> clientStubs = new ArrayList<LotteryManager>();
	// for (ServerDetail participant : participants) {
	// LotteryManager clientStub = getLotteryManagerClientStub(participant);
	// clientStub.freezeLottery();
	// clientStubs.add(clientStub);
	// }
	// synchronized (this.lottery) {
	// String winner = this.lottery.conductDraw();
	// for (LotteryManager clientStub : clientStubs) {
	// clientStub.setLotteryWinner(winner);
	// }
	// return winner;
	// }
	// }
	//
	// /**
	// * Evaluation function to print the load statistics.
	// */
	// @Override
	// public List<Double> getLoadStatistics() throws RemoteException {
	// List<Double> loadFactors = new ArrayList<Double>();
	// List<Double> loads = new ArrayList<Double>();
	// double totalLoad = 0.0;
	// List<ServerDetail> participants =
	// findAllParticipants(OBELIX_SERVICE_NAME);
	// for (ServerDetail participant : participants) {
	// int load = 0;
	// if (participant.getPID() == this.PID) {
	// load = this.localRequestCounter;
	// }
	// LotteryManager clientStub = getLotteryManagerClientStub(participant);
	// load = clientStub.getRequestCount();
	// loads.add(new Double(load));
	// totalLoad += load;
	// }
	//
	// for (Double load : loads) {
	// loadFactors.add(load / totalLoad);
	// }
	//
	// return loadFactors;
	// }
	//
	// /**
	// * Set lottery enter frequency to specified value.
	// *
	// * @param lotteryEnterFrequency
	// */
	// @Override
	// public void setLotteryEnterFrequency(int lotteryEnterFrequency)
	// throws RemoteException {
	// List<ServerDetail> participants =
	// findAllParticipants(OBELIX_SERVICE_NAME);
	// for (ServerDetail participant : participants) {
	// LotteryManager clientStub = getLotteryManagerClientStub(participant);
	// clientStub
	// .setLotteryEnterFrequency(this.PID, lotteryEnterFrequency);
	// }
	// }
	//
	// @Override
	// public void setLotteryEnterFrequency(Integer PID, int
	// lotteryEnterFrequency)
	// throws RemoteException {
	// lottery.lotteryEnterFrequency = lotteryEnterFrequency;
	// }
	//
	// /**
	// * Multicasts the current timestamp to other processes.
	// *
	// * @return Updated timestamp of the current process.
	// */
	//
	// @Override
	// public LamportClock notifyTimeStamp(LamportClock incomingTimeStamp)
	// throws RemoteException {
	// this.timeStamp.synchronizeTime(incomingTimeStamp);
	// return this.timeStamp;
	// }
	//
	// @Override
	// public int getRequestCount() throws RemoteException {
	// return this.localRequestCounter;
	// }
	//
	// @Override
	// public void freezeLottery() throws RemoteException {
	// this.lotteryFrozen = true;
	// }
	//
	// @Override
	// public void setLotteryWinner(String winnerID) throws RemoteException {
	// this.lotteryWinner = winnerID;
	// }
	//
	// @Override
	// public String getLotteryWinner(String clientID) throws RemoteException {
	// System.err.println("Sending lottery winner information.");
	// notifyEvent(clientID);
	// if (this.lotteryWinner == null) {
	// return null;
	// } else {
	// return this.lotteryWinner;
	// }
	// }

	@Override
	public void notifyScoreCaching(String serverID, EventCategories eventName)
			throws RemoteException {
		Set<EventCategories> cachedEvents = new HashSet<EventCategories>();
		if (this.scoreCacherList.containsKey(serverID)) {
			cachedEvents = this.scoreCacherList.get(serverID);
		}
		cachedEvents.add(eventName);
		this.scoreCacherList.put(serverID, cachedEvents);

	}

	@Override
	public void notifyResultCaching(String serverID, EventCategories eventName)
			throws RemoteException {
		Set<EventCategories> cachedResults = new HashSet<EventCategories>();
		if (this.scoreCacherList.containsKey(serverID)) {
			cachedResults = this.scoreCacherList.get(serverID);
		}
		cachedResults.add(eventName);
		this.scoreCacherList.put(serverID, cachedResults);

	}

	@Override
	public void notifyTallyCaching(String serverID, NationCategories nation)
			throws RemoteException {
		Set<NationCategories> cachedTallies = new HashSet<NationCategories>();
		if (this.tallyCacherList.containsKey(serverID)) {
			cachedTallies = this.tallyCacherList.get(serverID);
		}
		cachedTallies.add(nation);
		this.tallyCacherList.put(serverID, cachedTallies);
	}

	@Override
	public void invalidateScores(EventCategories eventName) {
		System.out.println("Invalidating score cache for " + eventName + " .");
		this.scoreCache.invalidateEntry(eventName);
	}

	@Override
	public void invalidateResults(EventCategories eventName) {
		System.out.println("Invalidating result cache for " + eventName + " .");
		this.resultCache.invalidateEntry(eventName);
	}

	@Override
	public void invalidateTallies(NationCategories nation) {
		System.out.println("Invalidating tally cache for " + nation + " .");
		this.tallyCache.invalidateEntry(nation);
	}

	private void cleanUpTallyCaches(NationCategories nation)
			throws RemoteException {
		for (String subscriber : this.tallyCacherList.keySet()) {
			try {
				ObelixInterface stub = this.getObelixSlaveStub(subscriber);
				stub.invalidateTallies(nation);
			} catch (ConnectException e) {
			} catch (ServerNotFoundException e) {
			}
		}
	}

	private void cleanUpResultCaches(EventCategories eventName)
			throws RemoteException {
		for (String subscriber : this.resultCacherList.keySet()) {
			try {
				ObelixInterface stub = this.getObelixSlaveStub(subscriber);
				stub.invalidateResults(eventName);
			} catch (ConnectException e) {
			} catch (ServerNotFoundException e) {
			}
		}
	}

	private void cleanUpScoreCaches(EventCategories eventName)
			throws RemoteException {
		for (String subscriber : this.scoreCacherList.keySet()) {
			try {
				ObelixInterface stub = this.getObelixSlaveStub(subscriber);
				stub.invalidateScores(eventName);
			} catch (ConnectException e) {
			} catch (ServerNotFoundException e) {
			}
		}
	}

	@Override
	public void refreshCaches(List<ServerDetail> obelixServersDetails)
			throws RemoteException {
		if (MASTER_PUSH) {
			synchronized (this.scoreCacherList) {
				this.scoreCacherList = new HashMap<String, Set<EventCategories>>();
			}

			synchronized (this.resultCacherList) {
				this.resultCacherList = new HashMap<String, Set<EventCategories>>();
			}

			synchronized (this.tallyCacherList) {
				this.tallyCacherList = new HashMap<String, Set<NationCategories>>();
			}

			for (ServerDetail obelixServerDetail : obelixServersDetails) {
				try {
					getObelixSlaveStub(obelixServerDetail).clearCaches();
				} catch (ServerNotFoundException e) {
				}
			}
		}
	}

	@Override
	public void clearCaches() throws RemoteException {
		synchronized (this.tallyCache) {
			this.tallyCache = new TallyCache();
		}

		synchronized (this.scoreCache) {
			this.scoreCache = new ScoreCache();
		}

		synchronized (this.resultCache) {
			this.resultCache = new ResultCache();
		}
	}
}

class HeartbeatNotifier implements Runnable {

	private Obelix obelixInstance;
	private static int BEAT_PERIOD = 3000;

	public HeartbeatNotifier() {
	}

	public HeartbeatNotifier(Obelix obelixInstance) {
		this.obelixInstance = obelixInstance;
	}

	@Override
	public void run() {
		while (true) {
			try {
				obelixInstance.beat(obelixInstance.getServerName());
				// System.err.println("Sending heartbeat msg.");
				Thread.sleep(BEAT_PERIOD);
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}