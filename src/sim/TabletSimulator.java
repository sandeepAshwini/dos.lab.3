package sim;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

import server.ServiceFinder;
import server.ServiceFinderInterface;
import util.LotteryManager;
import util.ServerDetail;
import base.EventCategories;
import base.OlympicException;
import client.Tablet;

/**
 * A simulator to deploy N tablets and perform tests on them.
 * 
 * @author aravind
 * 
 */
public class TabletSimulator {

	private static int DEFAULT_TABLET_LIMIT = 5;
	//private static int PROCESS_ORDERING_TABLET_LIMIT = 5;
	private static int JAVA_RMI_PORT;
	private static int DEFAULT_JAVA_RMI_PORT = 1099;
	private static String SERVICE_FINDER_NAME = "ServiceFinder";
	private static String OBELIX_SERVICE_NAME = "Obelix";
	private static ServiceFinderInterface serviceFinderStub;

	/**
	 * Creates and deploys N tablets as specified to be used for testing.
	 * 
	 * @param numberOfTablets
	 * @param obelixHost
	 * @return List<Tablet>
	 * @throws OlympicException
	 */
	private static List<Tablet> createTablets(int numberOfTablets,
			String serviceFinderHost, int serviceFinderPort)
			throws OlympicException {
		List<Tablet> tablets = new ArrayList<Tablet>();
		for (int i = 0; i < numberOfTablets; i++) {
			tablets.add(Tablet.deployTablet(serviceFinderHost,
					serviceFinderPort, JAVA_RMI_PORT));
		}
		return tablets;
	}

	/**
	 * Creates an associated TabletTester for each Tablet to work on a new
	 * thread and test the Tablet.
	 * 
	 * @param tablets
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private static void testTablets(List<Tablet> tablets)
			throws InterruptedException, IOException {
		List<Thread> threads = new ArrayList<Thread>();

		for (Tablet tablet : tablets) {
			TabletTester tester = new TabletTester(tablet);
			Thread thread = new Thread(tester);
			thread.start();
			threads.add(thread);
		}

		for (Thread thread : threads) {
			thread.join();
		}
	}

//	/**
//	 * Tests if the processes are ordered correctly by making requests from
//	 * tablets one at a time.
//	 * 
//	 * @param tablets
//	 * @throws RemoteException
//	 * @throws InterruptedException
//	 */
//	private static void testProcessOrdering(List<Tablet> tablets)
//			throws RemoteException, InterruptedException {
//		Tablet.pollTally = false;
//		LotteryManager clientStub = getObelixFrontendClientStub();
//		clientStub.setLotteryEnterFrequency(PROCESS_ORDERING_TABLET_LIMIT);
//		System.out.println(tablets.size());
//		System.out.println("Tablet "
//				+ tablets.get(PROCESS_ORDERING_TABLET_LIMIT - 1)
//						.getServerName()
//				+ " will always be entered into the lottery.");
//		for (int count = 0; count < 10; count++) {
//			for (Tablet tablet : tablets) {
//				tablet.getResults(EventCategories.STONE_LUGING);
//				Thread.sleep(500);
//			}
//		}
//		Tablet.pollTally = true;
//	}
//
//	public static LotteryManager getObelixFrontendClientStub()
//			throws RemoteException {
//		ServerDetail obelixDetail = serviceFinderStub
//				.getService(OBELIX_SERVICE_NAME);
//		return getLotteryManagerClientStub(obelixDetail);
//	}
//
//	/**
//	 * Sets up a client stub of type BullyElectableFrontend.
//	 * 
//	 * @param participant
//	 * @throws RemoteException
//	 */
//	private static LotteryManager getLotteryManagerClientStub(
//			ServerDetail participant) throws RemoteException {
//		Registry registry = null;
//		LotteryManager client = null;
//		registry = LocateRegistry.getRegistry(participant.getServiceAddress(),
//				participant.getServicePort());
//		try {
//			client = (LotteryManager) registry.lookup(participant
//					.getServerName());
//		} catch (NotBoundException e) {
//			e.printStackTrace();
//		}
//		return client;
//	}

	/**
	 * Sets up the {@link ServiceFinder} client stub used to register and lookup
	 * services.
	 * 
	 * @throws OlympicException
	 */
	private static void setupServiceFinderStub(String serviceFinderHost,
			int serviceFinderPort) throws OlympicException {
		Registry registry = null;
		try {
			registry = LocateRegistry.getRegistry(serviceFinderHost,
					serviceFinderPort);
			TabletSimulator.serviceFinderStub = (ServiceFinderInterface) registry
					.lookup(SERVICE_FINDER_NAME);
		} catch (Exception e) {
			throw new OlympicException("Could not set up Service Finder Stub.");
		}
	}

	/**
	 * @param args
	 * @throws OlympicException
	 * @throws IOException
	 * @throws NotBoundException
	 */
	public static void main(String[] args) throws OlympicException,
			IOException, NotBoundException {
		String serviceFinderHost = null;

		if(args.length < 1) {
			usage();
			System.exit(-1);
		} else {
			serviceFinderHost = args[0];
		}
		int serviceFinderPort = (args.length < 2) ? DEFAULT_JAVA_RMI_PORT
				: Integer.parseInt(args[1]);
		JAVA_RMI_PORT = (args.length < 3) ? DEFAULT_JAVA_RMI_PORT : Integer
				.parseInt(args[2]);
		setupServiceFinderStub(serviceFinderHost, serviceFinderPort);
		int numTablets = (args.length < 4) ? DEFAULT_TABLET_LIMIT : Integer
				.parseInt(args[3]);
		List<Tablet> tablets = createTablets(numTablets, serviceFinderHost,
				serviceFinderPort);
		try {
			// testProcessOrdering(tablets.subList(0,
			// PROCESS_ORDERING_TABLET_LIMIT));
			testTablets(tablets);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private static void usage() {
		System.out.println("java -cp ./bin/ -Djava.rmi.server.codebase=file:./bin/ sim.TabletSimulator"
				+ " <insert host address displayed by ServiceFinder>"
				+ " <insert port number displayed by ServiceFinder> [RMI_PORT] [number of tablets]");
	}
}
