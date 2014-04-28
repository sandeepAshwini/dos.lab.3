package server;

import java.io.IOException;
import java.rmi.ConnectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

import util.RegistryService;
import util.ServerDetail;
import util.ServiceComponent;
import base.Athlete;
import base.Event;
import base.OlympicException;

/**
 * Encapsulates the functions of Cacophonix.
 * Receives updates from the Olympic Games and relays them on to 
 * Obelix by singing.
 * @author sandeep
 *
 */
/**
 * @author aravind
 * 
 */
public class Cacophonix extends ServiceComponent implements CacophonixInterface {

	private static String CACOPHONIX_SERVICE_NAME = "Cacophonix";
	private static String OBELIX_SERVICE_NAME = "Obelix";
	private static String JAVA_RMI_HOSTNAME_PROPERTY = "java.rmi.server.hostname";
	private static String SERVICE_FINDER_HOST;
	private static int SERVICE_FINDER_PORT;
	private static int RETRY_LIMIT = 3;
	private static int RETRY_WAIT = 3000;

	// To prevent the server from being garbage collected.
	private static Cacophonix cacophonixServerInstance;

	private ObelixInterface clientStub;

	public Cacophonix(String serviceFinderHost, int serviceFinderPort) {
		super(CACOPHONIX_SERVICE_NAME, serviceFinderHost, serviceFinderPort);
	}

	public Cacophonix(ObelixInterface clientStub, String serviceFinderHost,
			int serviceFinderPort) {
		super(CACOPHONIX_SERVICE_NAME, serviceFinderHost, serviceFinderPort);
		this.clientStub = clientStub;
	}

	private static Cacophonix getCacophonixInstance() {
		if (Cacophonix.cacophonixServerInstance == null) {
			Cacophonix.cacophonixServerInstance = new Cacophonix(
					SERVICE_FINDER_HOST, SERVICE_FINDER_PORT);
		}
		return Cacophonix.cacophonixServerInstance;
	}

	/**
	 * Remote method called by Games when there is an update to the results of
	 * any event, that is when the event is completed. This in turn causes these
	 * results to be relayed on to Obelix, whose database is accordingly
	 * updated.
	 */
	public void updateResultsAndTallies(Event simulatedEvent)
			throws RemoteException {
		//TODO: Handle null properly here. Serious issue.
		System.err.println("Sending updateResultsAndTallies msg.");
		Cacophonix.getCacophonixInstance().setupClientInstance();
		if (clientStub != null) {
			clientStub.updateResultsAndTallies(simulatedEvent);
			return;
		}
	}

	/**
	 * Remote method called by Games when there is an update to scores in some
	 * event. The updates are relayed on to Obelix whose database is accordingly
	 * updated.
	 */
	public void updateCurrentScores(Event simulatedEvent,
			List<Athlete> currentScores) throws RemoteException {
		//TODO: Handle null properly here. Serious issue.
		System.err.println("Sending updatedCurrentScores msg.");
		Cacophonix.getCacophonixInstance().setupClientInstance();
		if (clientStub != null) {
			clientStub.updateCurrentScores(simulatedEvent.getName(),
					currentScores);
			return;
		}
	}

	/**
	 * Sets up client and server functions of Cacophonix.
	 * 
	 * @param args
	 * @throws OlympicException
	 */
	public static void main(String args[]) throws OlympicException {
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
		Cacophonix cacophonixInstance = Cacophonix.getCacophonixInstance();
		cacophonixInstance.setupClientInstance();
		try {
			RegistryService regService = new RegistryService();
			System.setProperty(JAVA_RMI_HOSTNAME_PROPERTY,
					regService.getLocalIPAddress());
			cacophonixInstance.setupServerInstance(regService);
		} catch (IOException e) {
			throw new OlympicException("Could not create Registry.", e);
		}
	}

	private static void usage() {
		System.out
				.println("java -cp ./bin/ -Djava.rmi.server.codebase=file:./bin/"
						+ " server.Cacophonix <insert host address displayed by ServiceFinder>"
						+ " <insert port number displayed by ServiceFinder> [RMI_PORT]");
	}

	/**
	 * Sets up Cacophonix server and registers with {@link ServiceFinder} which
	 * is used by Games updates scores and results.
	 * 
	 * @param clientStub
	 * @throws IOException
	 *             , OlympicException
	 */
	private void setupServerInstance(RegistryService regService)
			throws IOException, OlympicException {
		Registry registry = null;
		CacophonixInterface serverStub = (CacophonixInterface) UnicastRemoteObject
				.exportObject(Cacophonix.getCacophonixInstance(), 0);
		this.register(CACOPHONIX_SERVICE_NAME, regService.getLocalIPAddress(),
				JAVA_RMI_PORT);
		try {
			registry = LocateRegistry.getRegistry(JAVA_RMI_PORT);
			registry.rebind(this.getServerName(), serverStub);
			System.err.println("Registry Service running at "
					+ regService.getLocalIPAddress() + ":" + JAVA_RMI_PORT
					+ ".");
			System.err.println("Cacophonix ready.");
		} catch (RemoteException e) {
			registry = regService.setupLocalRegistry(JAVA_RMI_PORT);
			// registry = LocateRegistry.getRegistry(JAVA_RMI_PORT);
			registry.rebind(this.getServerName(), serverStub);
			System.err
					.println("New Registry Service created. Cacophonix ready.");
		}
	}

	/**
	 * Sets up the Obelix client stub by looking up {@link ServiceFinder}. This
	 * allows Cacophonix to 'sing' any received updates and hence inform Obelix
	 * of the same.
	 * 
	 * @return ObelixInterface
	 */
	private ObelixInterface setupClientInstance() {
		Registry registry = null;
		ObelixInterface clientStub = null;
		
		for (int i = 0; i < RETRY_LIMIT; i++) {
			try {
				ServerDetail obelixDetail = this.getServerDetails(
						OBELIX_SERVICE_NAME, this.getServerName());
				registry = LocateRegistry.getRegistry(
						obelixDetail.getServiceAddress(),
						obelixDetail.getServicePort());
				clientStub = (ObelixInterface) registry.lookup(obelixDetail
						.getServerName());
				Cacophonix.getCacophonixInstance().clientStub = clientStub;
			} catch (ConnectException e) {
				if (i >= RETRY_LIMIT) {
					e.printStackTrace();
				}
				try {
					Thread.sleep(RETRY_WAIT);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (NotBoundException e) {
				e.printStackTrace();
			}
		}
		
		return clientStub;
	}

//	/**
//	 * Propagates the conductLottery message to Obelix.
//	 */
//	@Override
//	public String conductLottery() throws RemoteException {
//		Cacophonix.getCacophonixInstance().setupClientInstance();
//		return clientStub.conductLottery();
//	}
}
