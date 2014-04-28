package client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.ConnectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import server.ObelixInterface;
import util.RegistryService;
import util.ServerDetail;
import util.ServiceComponent;
import base.Athlete;
import base.EventCategories;
import base.NationCategories;
import base.OlympicException;
import base.Printable;
import base.Results;
import base.Tally;

/**
 * Implementation of the client tablet.
 * 
 * @author sandeep
 * 
 */

public class Tablet extends ServiceComponent implements TabletInterface {
	// A local copy of the medal tallies.
	private Map<NationCategories, Tally> medalTallies;

	// Writer used to write to files instead of the console.
	private FileWriter writer = null;

	private static String JAVA_RMI_HOSTNAME_PROPERTY = "java.rmi.server.hostname";
	private static String SERVICE_FINDER_HOST;
	private static int SERVICE_FINDER_PORT;
	private static int RETRY_LIMIT = 3;
	private static int RETRY_WAIT = 3000;

	// Used only to test lamport clock synchronization.
	public static boolean pollTally = true;

	public Tablet(String serviceFinderHost, int serviceFinderPort) {
		super(CLIENT_BASE_NAME + UUID.randomUUID().toString(),
				serviceFinderHost, serviceFinderPort);
		this.clientID = this.serviceName;
		this.medalTallies = new HashMap<NationCategories, Tally>();
	}

	public Tablet(ObelixInterface obelixStub, String serviceFinderHost,
			int serviceFinderPort) {
		super(CLIENT_BASE_NAME + UUID.randomUUID().toString(),
				serviceFinderHost, serviceFinderPort);
		this.clientID = this.serviceName;
		this.medalTallies = new HashMap<NationCategories, Tally>();
		this.obelixStub = obelixStub;
	}

	public void setObelixStub(ObelixInterface obelixStub) {
		this.obelixStub = obelixStub;
	}

	/**
	 * Members specifying the server name(Obelix) and the base client
	 * identifier.
	 */
	private static String OBELIX_SERVICE_NAME = "Obelix";
	private static String CLIENT_BASE_NAME = "Client_";

	/**
	 * The server stub and the client ID(for event subscription).
	 */
	private ObelixInterface obelixStub;
	private String clientID;

	/**
	 * Control Variable to switch between subscribe and query modes.
	 */
	private volatile boolean resumeMenuLoop = false;

	/**
	 * Main method sets up the client tablet. Sets up the server stub for client
	 * pull. Also sets up the client as a server for server push mode. Finally
	 * calls the menu loop from where the server can be queried or events
	 * subscribed to.
	 * 
	 * @param args
	 * @throws OlympicException
	 */
	public static void main(String[] args) throws OlympicException {
		if (args.length < 1) {
			usage();
			System.exit(-1);
		} else {
			SERVICE_FINDER_HOST = args[0];
		}
		SERVICE_FINDER_PORT = (args.length < 2) ? DEFAULT_JAVA_RMI_PORT
				: Integer.parseInt(args[1]);
		JAVA_RMI_PORT = (args.length < 3) ? DEFAULT_JAVA_RMI_PORT : Integer
				.parseInt(args[2]);
		Tablet tabletInstance = deployTablet(SERVICE_FINDER_HOST,
				SERVICE_FINDER_PORT);
		if (tabletInstance != null) {
			tabletInstance.menuLoop();
		} else {
			throw new OlympicException("Could not instantiate tablet.");
		}
	}

	private static void usage() {
		System.out
				.println("java -cp ./bin/ -Djava.rmi.server.codebase=file:./bin/ client.Tablet"
						+ " <insert host address displayed by ServiceFinder>"
						+ " <insert port number displayed by ServiceFinder> [RMI_PORT]");
	}

	/**
	 * Deploys a single tablet instance, configuring it with the right host and
	 * client addresses.
	 * 
	 * @param args
	 * @return Tablet
	 * @throws OlympicException
	 */
	public static Tablet deployTablet(String serviceFinderHost,
			int serviceFinderPort) throws OlympicException {
		Tablet tabletInstance = new Tablet(serviceFinderHost, serviceFinderPort);
		try {
			RegistryService regService = new RegistryService();
			System.setProperty(JAVA_RMI_HOSTNAME_PROPERTY,
					regService.getLocalIPAddress());
			tabletInstance.setupTabletInstance(regService);
		} catch (IOException e) {
			throw new OlympicException("Registry could not be created.", e);
		}
		if (pollTally) {
			tabletInstance.setupTallyUpdateThread();
		}
		return tabletInstance;
	}

	public static Tablet deployTablet(String serviceFinderHost,
			int serviceFinderPort, int rmiPort) throws OlympicException {
		JAVA_RMI_PORT = rmiPort;
		Tablet tabletInstance = new Tablet(serviceFinderHost, serviceFinderPort);
		try {
			RegistryService regService = new RegistryService();
			System.setProperty(JAVA_RMI_HOSTNAME_PROPERTY,
					regService.getLocalIPAddress());
			tabletInstance.setupTabletInstance(regService);
		} catch (IOException e) {
			throw new OlympicException("Registry could not be created.", e);
		}
		if (pollTally) {
			tabletInstance.setupTallyUpdateThread();
		}
		return tabletInstance;
	}

	/**
	 * Simple command line interface to interact with the user.
	 * 
	 * @throws OlympicException
	 */
	private void menuLoop() throws OlympicException {
		try {
			while (true) {
				// String menuLine =
				// String.format("1. Get final results.\n2. Get medal tally.\n3. Get current score.\n4. Get Lottery Winner.\n5. Subscribe to updates.");
				String menuLine = String
						.format("1. Get final results.\n2. Get medal tally.\n3. Get current score.\n4. Get Lottery Winner.");
				this.printToConsole(menuLine, null, null);
				int choice = Integer.parseInt(getInput("Enter choice."));
				switch (choice) {
				case 1:
					this.getResults();
					break;
				case 2:
					this.getMedalTally();
					break;
				case 3:
					this.getCurrentScore();
					break;
				//case 4:
				//	this.getLotteryWinner();
				//	break;
				// case 5: this.subscribeTo();
				// this.waitToResume();
				// this.resumeMenuLoop = false; break;
				default:
					this.printToConsole("Not a valid menu option.", null, null);
				}
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Sets up the Obelix server stub. Also sets up the Tablet server stub for
	 * server push mode.
	 * 
	 * @param obelixHost
	 * @param tabletHost
	 * @return Tablet
	 * @throws IOException
	 */
	private void setupTabletInstance(RegistryService regService)
			throws IOException, OlympicException {
		this.setupObelixStub();
		this.setupTabletServer(regService);
	}

	private void setupObelixStub() throws RemoteException {
		for (int i = 0; i < RETRY_LIMIT; i++) {
			try {
				ServerDetail obelixDetail = this.getServerDetails(OBELIX_SERVICE_NAME, this.getServerName());
				ObelixInterface obelixStub = connectToObelix(obelixDetail);
				this.setObelixStub(obelixStub);
			} catch (ConnectException e) {
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
	}

	/**
	 * Returns the Server(Obelix) stub after doing the required lookup in the
	 * RMI Registry and creating the stub.
	 * 
	 * @param obelixHost
	 * @return ObelixInterface
	 * @throws ConnectException 
	 */
	private static ObelixInterface connectToObelix(ServerDetail obelixDetail) throws ConnectException {
		Registry registry = null;
		ObelixInterface obelixStub = null;

		try {
			registry = LocateRegistry.getRegistry(
					obelixDetail.getServiceAddress(),
					obelixDetail.getServicePort());
			obelixStub = (ObelixInterface) registry.lookup(obelixDetail
					.getServerName());
		} catch (ConnectException e) {
			throw e;
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}

		return obelixStub;
	}

	/**
	 * Sets up the tablet as a server to receive updates from Obelix.
	 * 
	 * @param host
	 * @throws IOException
	 */
	private void setupTabletServer(RegistryService regService)
			throws IOException, OlympicException {
		Registry registry = null;
		TabletInterface tabletStub = (TabletInterface) UnicastRemoteObject
				.exportObject(this, 0);
		this.register(clientID, regService.getLocalIPAddress(), JAVA_RMI_PORT);
		try {
			registry = LocateRegistry.getRegistry(JAVA_RMI_PORT);
			registry.rebind(this.getServerName(), tabletStub);
			System.err.println("Registry Service running at : "
					+ regService.getLocalIPAddress() + ":" + JAVA_RMI_PORT
					+ ".");
			System.err.println("Tablet ready.");
		} catch (RemoteException e) {
			registry = regService.setupLocalRegistry(JAVA_RMI_PORT);
			// registry = LocateRegistry.getRegistry(JAVA_RMI_PORT);
			registry.rebind(this.getServerName(), tabletStub);
			System.err.println("New Registry Service created. Tablet ready");
		}
	}

	/**
	 * Allows subscription to events. Takes the Event Name as user input from
	 * the CLI.
	 * 
	 * @throws OlympicException
	 */
	private void subscribeTo() throws OlympicException {
		EventCategories eventName = EventCategories
				.valueOf(getInput("Event name?"));
		subscribeTo(eventName);
	}

	public void subscribeTo(EventCategories eventType) throws OlympicException {
		try {
			RegistryService regService = new RegistryService();
			obelixStub.registerClient(clientID, regService.getLocalIPAddress(),
					eventType);
			printToConsole(
					"Successfully subscribed to " + eventType.getCategory()
							+ ".", null, null);
		} catch (IOException e) {
			throw new OlympicException("Could not subscribe.", e);
		}
	}

	/**
	 * Method to retrieve input from user. Synchronized as results should not be
	 * written into the console while waiting for user input.
	 * 
	 * @param msg
	 * @return
	 */
	private synchronized String getInput(String msg) {
		try {
			this.printToConsole(msg, null, null);
		} catch (IOException e) {
			e.printStackTrace();
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				System.in));
		String input = null;

		try {
			input = reader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return input;
	}

	/**
	 * Queries the server for the results of a particular event. The event is
	 * accepted as input from the CLI.
	 * 
	 * @throws RemoteException
	 */
	private void getResults() throws RemoteException {
		String eventName = getInput("Event name");
		EventCategories eventType = EventCategories.valueOf(eventName
				.toUpperCase());
		this.getResults(eventType);
	}

	public void getResults(EventCategories eventType) throws RemoteException {
		synchronized (this.obelixStub) {
			this.setupObelixStub();
			Results result = obelixStub.getResults(eventType, this.getServerName());
			if (result != null) {
				this.printCurrentResult(eventType, result);
			} else {
				try {
					printToConsole("Event " + eventType.getCategory()
							+ " hasn't completed yet.", null, null);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Queries the server for the medal tally of a particular team. The team
	 * name is accepted as input from the CLI.
	 * 
	 * @throws RemoteException
	 */

	private void getMedalTally() throws RemoteException {
		String teamName = getInput("Team name");
		NationCategories nation = NationCategories.valueOf(teamName
				.toUpperCase());
		this.getMedalTally(nation);
	}

	public void getMedalTally(NationCategories nation) throws RemoteException {
		synchronized (this.medalTallies) {
			if (pollTally == false) {
				this.updateMedalTallies();
			}
			Tally medalTally = this.medalTallies.get(nation);
			this.printCurrentTally(nation, medalTally);
		}
	}

	/**
	 * Request medal tallies for all nations from Obelix.
	 * 
	 * @throws RemoteException
	 */
	public void updateMedalTallies() throws RemoteException {
		synchronized (this.medalTallies) {
			for (NationCategories nation : NationCategories.values()) {
				synchronized(this.obelixStub) {
					this.setupObelixStub();
					this.medalTallies.put(
							nation,
							this.obelixStub.getMedalTally(nation,
									this.getServerName()));
				}				
			}
		}
	}

	/**
	 * Setup the tally updater thread.
	 */
	public void setupTallyUpdateThread() {
		Thread thread = new Thread(new TallyUpdater(this), "TallyUpdaterThread");
		thread.start();
	}

	/**
	 * Queries the server for the current scores of a particular event. The
	 * event is accepted as input from the CLI.
	 * 
	 * @throws RemoteException
	 */

	private void getCurrentScore() throws RemoteException {
		String eventName = getInput("Event Name");
		EventCategories eventType = EventCategories.valueOf(eventName
				.toUpperCase());
		this.getCurrentScore(eventType);
	}

	public void getCurrentScore(EventCategories eventType)
			throws RemoteException {
		synchronized (this.obelixStub) {
			this.setupObelixStub();
			List<Athlete> scores = this.obelixStub.getCurrentScores(eventType,
					this.getServerName());
			if (scores != null && scores.size() != 0) {
				printCurrentScore(eventType, scores);
			} else {
				try {
					printToConsole("Event " + eventType.getCategory()
							+ " hasn't started yet.", null, null);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

//	public void getLotteryWinner() throws RemoteException {
//		synchronized(this.obelixStub) {
//			this.setupObelixStub();
//			String winner = this.obelixStub.getLotteryWinner(this.getServerName());
//			try {
//
//				if (winner != null) {
//					if (winner.equals(this.getServerName())) {
//						printToConsole(
//								"You are the lucky winner. Congratulations.", null,
//								null);
//
//					} else {
//						printToConsole("The lucky winner is " + winner + ".", null,
//								null);
//					}
//
//				} else {
//					printToConsole("Lottery Results are not yet available.", null,
//							null);
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//	}

	/**
	 * Pretty prints the results of the specified event to the console.
	 * 
	 * @param eventName
	 * @param result
	 */
	private void printCurrentResult(EventCategories eventName, Results result) {
		String header = String.format("Results for %s.",
				eventName.getCategory());
		try {
			this.printToConsole(header, result.convertToList(), null);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Pretty prints the medal tally of the specified team to the console.
	 * 
	 * @param eventName
	 * @param result
	 */
	private void printCurrentTally(NationCategories teamName, Tally medalTally) {
		String header = String.format("Medal Tally for %s.",
				teamName.getCategory());
		try {
			this.printToConsole(header, medalTally.convertToList(), null);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Pretty prints the current scores of the specified event to the console.
	 * 
	 * @param eventName
	 * @param result
	 */
	private void printCurrentScore(EventCategories eventName,
			List<Athlete> scores) {
		String header = String
				.format("Scores for %s.", eventName.getCategory());
		List<Printable> printList = new ArrayList<Printable>();
		for (Athlete athlete : scores) {
			printList.add(athlete);
		}
		try {
			this.printToConsole(header, printList, null);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Calls made by the Obelix server for server push mode.
	 */
	/**
	 * The server calls this function when the scores of a subscribed event
	 * change. When called, the updated scores are printed to console.
	 */
	@Override
	public void updateScores(EventCategories eventName, List<Athlete> scores)
			throws RemoteException {
		printCurrentScore(eventName, scores);
	}

	/**
	 * The server calls this function when the final results of a subscribed
	 * event are available, that is, the event is completed. When called, the
	 * final results are printed to console. Then, the subscription is
	 * effectively over and the user is taken back to the menu.
	 */
	@Override
	public void updateResults(EventCategories eventName, Results result)
			throws RemoteException {
		printCurrentResult(eventName, result);
		this.resumeMenuLoop = true;
	}

	/**
	 * Synchronized method to print to console. This method needs to be thread
	 * safe as only one thing should be printed to the console at any given
	 * time. Also, no output should be dumped when we are waiting for user
	 * input.
	 * 
	 * @param header
	 * @param printList
	 * @param footer
	 * @throws IOException
	 */
	private synchronized void printToConsole(String header,
			List<Printable> printList, String footer) throws IOException {
		if (this.writer == null) {
			if (header != null)
				System.out.println(header);

			if (printList != null) {
				for (Printable printObject : printList) {
					printObject.printContents();
				}
			}
			if (footer != null) {
				System.out.println(footer);
			}
			System.out.println();

		} else {
			if (header != null)
				writer.write(header + "\n");

			if (printList != null) {
				for (Printable printObject : printList) {
					printObject.writeToFile(this.writer);
				}
			}
			if (footer != null)
				writer.write(footer + "\n");

			writer.write("\n");
			writer.flush();
		}

	}

	/**
	 * Wait loop entered into when a subscription is registered. Only
	 * subscription updates are allowed to be printed durung this time. When the
	 * event is completed, the user is once again shown the menu.
	 */
	private void waitToResume() {
		while (this.resumeMenuLoop == false)
			;
	}

	public void setOut(String fileName) throws IOException {
		this.writer = new FileWriter(new File(fileName));
	}
}

/**
 * Private tally updater that runs on a separate thread and periodically
 * requests medal tallies from Obelix and updates the local copy in Tablet.
 * 
 * @author aravind
 * 
 */
class TallyUpdater implements Runnable {

	private Tablet tabletInstance;
	private int updatePeriod;

	public TallyUpdater() {
	}

	public TallyUpdater(Tablet tabletInstance) {
		this.tabletInstance = tabletInstance;
		this.updatePeriod = 3000;
	}

	public TallyUpdater(Tablet tabletInstance, int updatePeriod) {
		this.tabletInstance = tabletInstance;
		this.updatePeriod = updatePeriod;
	}

	@Override
	public void run() {
		while (true) {
			try {
				tabletInstance.updateMedalTallies();
				Thread.sleep(updatePeriod);
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}