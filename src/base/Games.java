package base;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;

import server.CacophonixInterface;
import util.ServerDetail;
import util.ServiceComponent;

/**
 * Class encapsulates the entire Olympic Games.
 * 
 * @author sandeep
 * 
 */

public class Games extends ServiceComponent {

	/**
	 * Game attributes.
	 */
	private ArrayList<Event> events;
	private String venue;
	private String year;
	private int currentEvent;

	private static String CACOPHONIX_SERVICE_NAME = "Cacophonix";

	public Games() {
	}

	public Games(String venue, String year) {
		this.events = new ArrayList<Event>();
		this.year = year;
		this.venue = venue;
		currentEvent = 0;

		for (EventCategories eventName : EventCategories.values()) {
			this.events.add(new Event(eventName));
		}
	}

	/**
	 * General getters.
	 */
	public String getGameVenue() {
		return this.venue;
	}

	public String getGameYear() {
		return this.year;
	}

	public ArrayList<Event> getEvents() {
		return this.events;
	}

	public Event getNextEvent() {
		return this.events.get(this.currentEvent);
	}

	/**
	 * Prints a welcome message.
	 */
	public void printGameIntro() {
		System.out.println("Welcome to the Stone Olympics of " + this.year
				+ " at " + this.venue + ".");
	}

	/**
	 * Gets the next event and simulates it on a new thread.
	 * 
	 * @return Event
	 */
	public Event simulateNextEvent() {
		Event currentEvent = events.get(this.currentEvent++);
		Thread thread = new Thread(currentEvent);
		thread.start();
		return currentEvent;
	}

	/**
	 * The Game class acts as a server for Cacophonix. The main method creates a
	 * new Game object and simulates the games. Events are simulated one after
	 * the other with a pre defined break between them. Scores of the current
	 * event are updated every 5 seconds by sending a message to Cacophonix.
	 * Once an event is completed, the Results and Medal Tallies are updated by
	 * sending a message to Cacophonix.
	 * 
	 * At the end of the simulation it initiates the lottery by sending a
	 * message to Cacophonix.
	 * 
	 * @param args
	 * @throws OlympicException
	 */
	public static void main(String[] args) throws OlympicException {
		long TIME_DELAY = 20 * 1000;
		long SLEEP_DURATION = (long) 5.1 * 1000;
		String serviceFinderHost = null;

		if(args.length < 1) {
			usage();
			System.exit(-1);
		} else {
			serviceFinderHost = args[0];
		}
		
		int serviceFinderPort = (args.length < 2) ? DEFAULT_JAVA_RMI_PORT : Integer
				.parseInt(args[1]);
		JAVA_RMI_PORT = (args.length < 3) ? DEFAULT_JAVA_RMI_PORT : Integer
				.parseInt(args[2]);
		Games game = new Games("Pompeii", "48 BC");
		game.setServiceFinderAddress(serviceFinderHost, serviceFinderPort);
		int numEvents = game.events.size();

		game.printGameIntro();
		try {
			ServerDetail cacophonixDetail = game
					.getServerDetails(CACOPHONIX_SERVICE_NAME);
			Registry registry = LocateRegistry.getRegistry(
					cacophonixDetail.getServiceAddress(),
					cacophonixDetail.getServicePort());
			final CacophonixInterface stub = (CacophonixInterface) registry
					.lookup(cacophonixDetail.getServerName());
			for (int i = 0; i < numEvents; i++) {
				Event simulatedEvent = game.simulateNextEvent();
				System.err.println("Beginning "
						+ simulatedEvent.getName().getCategory() + ".");
				if (simulatedEvent != null) {
					while (!simulatedEvent.isCompleted()) {
						System.err.println("Sending latest scores.");
						ArrayList<Athlete> currentScores = simulatedEvent
								.getScores();
						stub.updateCurrentScores(simulatedEvent, currentScores);
						Thread.sleep(SLEEP_DURATION);
					}
					System.err.println("Sending final results.");
					stub.updateResultsAndTallies(simulatedEvent);
				}
				Thread.sleep(TIME_DELAY);
			}
			//System.err.println("Games ended. Now conducting lucky raffle.");
			//printCongratulatoryMsg(stub.conductLottery());

		} catch (NotBoundException e) {
			throw new OlympicException("Cannot find Cacophonix.", e);
		} catch (RemoteException e) {
			throw new OlympicException("Error while broadcasting.", e);
		} catch (InterruptedException e) {
			throw new OlympicException("Games Interrupted.", e);
		}

	}
	
	private static void usage() {
		System.out.println("java -cp ./bin/ base.Games"
				+ " <insert host address displayed by ServiceFinder>"
				+ " <insert port number displayed by ServiceFinder>"); 
	}

	/**
	 * Utility function to print a congratulatory message at the end of the
	 * lottery.
	 * 
	 * @param winner
	 */
	public static void printCongratulatoryMsg(String winner) {
		if(winner == null){
			System.out
			.println("No participants in the lottery.");
		}
		System.out
				.println(String
						.format("Congratulations %s. You win a hunting trip for boars with Asterix, "
								+ "followed by an invitation to the grand feast for the gold medal winners that "
								+ "will be organized by chief Vitalstatistix.",
								winner));
		System.out.println("Courtesy: Rotten Apples Smart Stone Company.");

	}

}
