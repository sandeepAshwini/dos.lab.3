package server;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import java.util.UUID;

import util.BullyElectedBerkeleySynchronized;
import util.RegistryService;
import util.ServiceComponent;
import base.Athlete;
import base.Event;
import base.EventCategories;
import base.MedalCategories;
import base.NationCategories;
import base.OlympicException;
import base.Results;
import base.Tally;

/**
 * Encapsulates the backend database process responsible for storage and
 * retrieval of updated scores, results and medal tallies. The database is
 * stored as files on disk.
 * 
 * @author aravind
 * 
 */
public class Orgetorix extends ServiceComponent implements OrgetorixInterface {
	private static String JAVA_RMI_HOSTNAME_PROPERTY = "java.rmi.server.hostname";
	private static String FILE_LOCATION = "./";
	private static String ORGETORIX_SERVICE_NAME = "Orgetorix";
	private static String SERVICE_FINDER_HOST;
	private static int SERVICE_FINDER_PORT;

	private static Orgetorix orgetorixServerInstance;
	private String resultFileName;
	private String tallyFileName;
	private String scoreFileName;
	private String dbName;

	public Orgetorix(String serviceFinderHost, int serviceFinderPort) {
		super(ORGETORIX_SERVICE_NAME, serviceFinderHost, serviceFinderPort);
		this.dbName = UUID.randomUUID().toString();
		this.resultFileName = FILE_LOCATION + "Results" + this.dbName;
		this.tallyFileName = FILE_LOCATION + "Tallies" + this.dbName;
		this.scoreFileName = FILE_LOCATION + "Scores" + this.dbName;
		try {
			this.initializeDatabase();
		} catch (RemoteException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Initializes the database with empty records of scores, results and
	 * tallies.
	 * 
	 * @throws RemoteException
	 */
	private void initializeDatabase() throws RemoteException {
		this.writeToDatabase(new HashSet<Event>(), this.resultFileName);
		Map<NationCategories, Tally> medalTallies = new HashMap<NationCategories, Tally>();
		for (NationCategories nation : NationCategories.values()) {
			medalTallies.put(nation, new Tally());
			//medalTallies.get(nation).setTimestamp(this.getTime());
			medalTallies.get(nation).setTimestamp(System.currentTimeMillis());
		}
		this.writeToDatabase(medalTallies, this.tallyFileName);
		Map<EventCategories, ArrayList<Athlete>> scores = new HashMap<EventCategories, ArrayList<Athlete>>();
		for (EventCategories event : EventCategories.values()) {
			scores.put(event, new ArrayList<Athlete>());
			for (Athlete athleteScore : scores.get(event)) {
				//athleteScore.setTimestamp(this.getTime());
				athleteScore.setTimestamp(System.currentTimeMillis());
			}
		}
		this.writeToDatabase(scores, this.scoreFileName);
	}

	/**
	 * Updates the results and tallies of a specified event in the database.
	 * 
	 * @param simulatedEvent
	 * @throws RemoteException
	 */
	@Override
	public void updateResultsAndTallies(Event simulatedEvent)
			throws RemoteException {
		updateResults(simulatedEvent);
		updateMedalTallies(simulatedEvent.getResult());
	}

	/**
	 * Updates the results of a specified event in the database.
	 * 
	 * @param completedEvent
	 * @throws RemoteException
	 */
	private void updateResults(Event completedEvent) throws RemoteException {
		Set<Event> completedEvents = readResultFile();
		completedEvents.add(completedEvent);

		for (Event event : completedEvents) {
			//event.getResult().setTimestamp(this.getTime());
			event.getResult().setTimestamp(System.currentTimeMillis());
		}

		writeToDatabase(completedEvents, this.resultFileName);
	}

	/**
	 * Updates the medal tallies in the database at the end of an event.
	 * 
	 * @param eventResult
	 * @throws RemoteException
	 */
	private void updateMedalTallies(Results eventResult) throws RemoteException {
		Map<NationCategories, Tally> medalTallies = readTallyFile();
		for (MedalCategories medalType : MedalCategories.values()) {
			medalTallies.get(eventResult.getTeam(medalType)).incrementTally(
					medalType);
		}

		for (NationCategories nation : medalTallies.keySet()) {
			//medalTallies.get(nation).setTimestamp(this.getTime());
			medalTallies.get(nation).setTimestamp(System.currentTimeMillis());
		}

		writeToDatabase(medalTallies, this.tallyFileName);
	}

	/**
	 * Updates the current scores for a specified event type in the database.
	 * 
	 * @param eventType
	 * @param currentScores
	 * @throws RemoteException
	 */
	@Override
	public void updateCurrentScores(EventCategories eventType,
			List<Athlete> currentScores) throws RemoteException {
		Map<EventCategories, ArrayList<Athlete>> scores = readScoreFile();
		for (Athlete athleteScore : currentScores) {
			//athleteScore.setTimestamp(this.getTime());
			athleteScore.setTimestamp(System.currentTimeMillis());
		}
		scores.put(eventType, (ArrayList<Athlete>) currentScores);

		writeToDatabase(scores, this.scoreFileName);
	}

	/**
	 * Retreives the medal tally for a specific team name from the database.
	 * 
	 * @param teamName
	 * @return The updated medal tally for teamName.
	 * @throws RemoteException
	 */
	@Override
	public Tally getMedalTally(NationCategories teamName)
			throws RemoteException {
		Map<NationCategories, Tally> medalTallies = readTallyFile();
		return medalTallies.get(teamName);

	}

	/**
	 * Retreived the results for a specified event from the database.
	 * 
	 * @param eventName
	 * @return The final results for eventName if the event has ended.
	 * @throws RemoteException
	 */
	@Override
	public Results getResults(EventCategories eventName) throws RemoteException {
		Set<Event> completedEvents = readResultFile();
		for (Event event : completedEvents) {
			if (event.getName() == eventName) {
				return event.getResult();
			}
		}
		return null;
	}

	/**
	 * Retreives the latest scores for a specified event from the database.
	 * 
	 * @param eventName
	 * @return The latest scores for eventName.
	 * @throws RemoteException
	 */
	@Override
	public List<Athlete> getCurrentScores(EventCategories eventName)
			throws RemoteException {
		Map<EventCategories, ArrayList<Athlete>> scores = readScoreFile();
		return scores.get(eventName);
	}

	/**
	 * Utility function to read the result file.
	 */
	private Set<Event> readResultFile() {
		Set<Event> completedEvents = (Set<Event>) readFromDatabase(this.resultFileName);
		return completedEvents;
	}

	/**
	 * Utility function to read the medal tally file.
	 */
	private Map<NationCategories, Tally> readTallyFile() {
		Map<NationCategories, Tally> medalTallies = (Map<NationCategories, Tally>) readFromDatabase(this.tallyFileName);
		return medalTallies;
	}

	/**
	 * Utility function to read the score file.
	 */
	private Map<EventCategories, ArrayList<Athlete>> readScoreFile() {
		Map<EventCategories, ArrayList<Athlete>> scores = (Map<EventCategories, ArrayList<Athlete>>) readFromDatabase(this.scoreFileName);
		return scores;
	}

	/**
	 * Utility function to serialize an object to a file (database).
	 * 
	 * @param object
	 * @param filename
	 */
	private synchronized void writeToDatabase(Object object, String filename) {
		try {
			FileOutputStream fileOut = new FileOutputStream(filename);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(object);
			out.flush();
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	/**
	 * Utility function to deserialize an object from a file.
	 * 
	 * @param filename
	 * @return
	 */
	private synchronized Object readFromDatabase(String filename) {
		Object object = null;
		try {
			FileInputStream fileIn = new FileInputStream(filename);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			object = in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException c) {
			System.out.println("Class not found");
			c.printStackTrace();
		}
		return object;
	}

	private static Orgetorix getOrgetorixInstance() {
		if (Orgetorix.orgetorixServerInstance == null) {
			Orgetorix.orgetorixServerInstance = new Orgetorix(
					SERVICE_FINDER_HOST, SERVICE_FINDER_PORT);
		}
		return Orgetorix.orgetorixServerInstance;
	}

	public void addParticipant(String participantID) throws RemoteException {
	}

	/**
	 * Sets up the Orgetorix server stub and registers itself with
	 * {@link ServiceFinder}
	 * 
	 * @param regService
	 * @throws IOException
	 * @throws OlympicException
	 */
	private void setupOrgetorixServer(RegistryService regService)
			throws IOException, OlympicException {
		Registry registry = null;

		this.register(ORGETORIX_SERVICE_NAME, regService.getLocalIPAddress(),
				JAVA_RMI_PORT);
		OrgetorixInterface serverStub = (OrgetorixInterface) UnicastRemoteObject
				.exportObject(Orgetorix.getOrgetorixInstance(), 0);
		try {
			registry = LocateRegistry.getRegistry(JAVA_RMI_PORT);
			registry.rebind(this.getServerName(), serverStub);
			System.err.println("Registry Service running at "
					+ regService.getLocalIPAddress() + ":" + JAVA_RMI_PORT
					+ ".");
			System.err.println("Orgetorix ready.");
		} catch (RemoteException e) {
			registry = regService.setupLocalRegistry(JAVA_RMI_PORT);
			// registry = LocateRegistry.getRegistry(JAVA_RMI_PORT);
			registry.rebind(this.getServerName(), serverStub);
			System.err
					.println("New Registry Service created. Orgetorix ready.");
		}
	}

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
		Orgetorix orgetorixInstance = Orgetorix.getOrgetorixInstance();

		try {
			RegistryService regService = new RegistryService();
			System.setProperty(JAVA_RMI_HOSTNAME_PROPERTY,
					regService.getLocalIPAddress());
			orgetorixInstance.setupOrgetorixServer(regService);
			//orgetorixInstance.initiateElection();
		} catch (IOException e) {
			throw new OlympicException(
					"Registry Service could not be created.", e);
		}
	}

	private static void usage() {
		System.out.println("java -cp ./bin/ -Djava.rmi.server.codebase=file:./bin/"
				+ " server.Orgetorix <insert host address displayed by ServiceFinder>"
				+ " <insert port number displayed by ServiceFinder> [RMI_PORT]");
	}
}
