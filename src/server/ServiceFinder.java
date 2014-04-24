package server;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import util.RegistryService;
import util.ServerDetail;
import base.OlympicException;

/**
 * Encapsulates a centralized service discovery process.
 * 
 * @author aravind
 * 
 */
public class ServiceFinder implements ServiceFinderInterface {

	private static Random random;
	private static ServiceFinder serviceFinderInstance;
	private static String JAVA_RMI_HOSTNAME_PROPERTY = "java.rmi.server.hostname";
	private static int JAVA_RMI_PORT;
	private static int DEFAULT_JAVA_RMI_PORT = 1099;

	private List<ServerDetail> services = new ArrayList<ServerDetail>();

	public ServiceFinder() {
		random = new Random();
	}

	public static void main(String[] args) throws OlympicException {
		JAVA_RMI_PORT = (args.length < 1) ? DEFAULT_JAVA_RMI_PORT : Integer
				.parseInt(args[0]);
		ServiceFinder serviceFinderInstance = ServiceFinder
				.getServiceFinderInstance();
		try {
			RegistryService regService = new RegistryService();
			System.setProperty(JAVA_RMI_HOSTNAME_PROPERTY,
					regService.getLocalIPAddress());
			serviceFinderInstance.setupServiceFinder(regService);
		} catch (IOException e) {
			throw new OlympicException(
					"Registry Service could not be created.", e);
		}
	}

	private static ServiceFinder getServiceFinderInstance() {
		if (ServiceFinder.serviceFinderInstance == null) {
			ServiceFinder.serviceFinderInstance = new ServiceFinder();
		}
		return ServiceFinder.serviceFinderInstance;
	}

	/**
	 * Sets up the ServiceFinder server that can be used by all other processes
	 * to discover other services.
	 * 
	 * @param regService
	 * @throws IOException
	 * @throws OlympicException
	 */
	private void setupServiceFinder(RegistryService regService)
			throws IOException, OlympicException {
		Registry registry = null;
		String SERVER_NAME = "ServiceFinder";
		ServiceFinderInterface serverStub = null;

		try {
			serverStub = (ServiceFinderInterface) UnicastRemoteObject
					.exportObject(ServiceFinder.getServiceFinderInstance(), 0);
			registry = LocateRegistry.getRegistry(JAVA_RMI_PORT);
			registry.rebind(SERVER_NAME, serverStub);
			System.err.println("Registry Service running at "
					+ regService.getLocalIPAddress() + ":" + JAVA_RMI_PORT
					+ ".");
			System.err.println("ServiceFinder ready.");
		} catch (RemoteException e) {
			registry = regService.setupLocalRegistry(JAVA_RMI_PORT);
//			registry = LocateRegistry.getRegistry(JAVA_RMI_PORT);
			registry.rebind(SERVER_NAME, serverStub);
			System.err
					.println("New Registry Service created. ServiceFinder ready.");
		}
	}

	/**
	 * Registers a server offering a specified service.
	 * 
	 * @param serviceName
	 * @param PID
	 * @param address
	 */
	@Override
	public void registerService(String serviceName, int PID, String address,
			int rmiPort) throws RemoteException {
		synchronized (this.services) {
			this.services.add(new ServerDetail(serviceName, PID, address,
					rmiPort));
		}
	}

	/**
	 * Retrieves the service matching the specified service name. If multiple
	 * servers offer same service, returns any one of them randomly, all having
	 * equal chance to be selected.
	 * 
	 * @return The server details for the specified service name.
	 * @param serviceName
	 */
	@Override
	public ServerDetail getService(String serviceName) throws RemoteException {
		List<ServerDetail> matchingServices = getServices(serviceName);
		int num = random.nextInt(matchingServices.size());
		ServerDetail pickedService = matchingServices.get(num);
		System.out.println("Resolved " + serviceName + " to " + pickedService.getServerName() + ".");
		return pickedService;
	}

	/**
	 * Retrieves all servers matching the specified service name.
	 * 
	 * @return The server details of all servers offering the specified service.
	 */
	@Override
	public List<ServerDetail> getServices(String serviceName)
			throws RemoteException {
		List<ServerDetail> matchingServices = new ArrayList<ServerDetail>();
		synchronized (this.services) {
			for (ServerDetail curServerDetail : this.services) {
				if (curServerDetail.getServiceName().equals(serviceName)) {
					matchingServices.add(curServerDetail);
				}
			}
		}
		return matchingServices;
	}
	
	
	public ServerDetail getServer(String serverName)
			throws RemoteException {
		synchronized (this.services) {
			for (ServerDetail curServerDetail : this.services) {
				if (curServerDetail.getServerName().equals(serverName)) {
					return curServerDetail;
				}
			}
		}
		return null;
	}
	
}
