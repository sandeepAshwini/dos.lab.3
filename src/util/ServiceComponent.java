package util;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Random;

import server.ServiceFinder;
import server.ServiceFinderInterface;
import base.OlympicException;

/**
 * Represents an abstract service.
 * 
 * @author aravind
 * 
 */
public abstract class ServiceComponent {

	private static Random random;
	private ServiceFinderInterface serviceFinderStub;
	private String serviceFinderHost;
	private int serviceFinderPort;
	protected int PID;
	protected String serviceName;

	private static String SERVICE_FINDER_NAME = "ServiceFinder";
	protected static int JAVA_RMI_PORT;
	protected static int DEFAULT_JAVA_RMI_PORT = 1099;

	public ServiceComponent() {
		random = new Random();
		this.PID = Math.abs(random.nextInt());
	}

	public ServiceComponent(String serviceName, String serviceFinderHost,
			int serviceFinderPort) {
		this.serviceName = serviceName;
		try {
			setServiceFinderAddress(serviceFinderHost, serviceFinderPort);
		} catch (OlympicException e) {
			e.printStackTrace();
		}
		random = new Random();
		this.PID = Math.abs(random.nextInt());
	}

	/**
	 * Sets the service finder host used to register and lookup services.
	 * 
	 * @param serviceFinderHost
	 * @throws OlympicException
	 */
	public void setServiceFinderAddress(String serviceFinderHost,
			int serviceFinderPort) throws OlympicException {
		this.serviceFinderPort = serviceFinderPort;
		this.serviceFinderHost = serviceFinderHost;
		setupServiceFinderStub();
	}

	/**
	 * Sets up the {@link ServiceFinder} client stub used to register and lookup
	 * services.
	 * 
	 * @throws OlympicException
	 */
	private void setupServiceFinderStub() throws OlympicException {
		Registry registry = null;
		try {
			registry = LocateRegistry.getRegistry(serviceFinderHost,
					serviceFinderPort);
			ServiceFinderInterface serviceFinderStub = (ServiceFinderInterface) registry
					.lookup(SERVICE_FINDER_NAME);
			this.serviceFinderStub = serviceFinderStub;
		} catch (Exception e) {
			throw new OlympicException("Could not set up Service Finder Stub.");
		}
	}

	/**
	 * @return The server name for the current server process.
	 */
	public String getServerName() {
		return this.serviceName + this.PID;
	}

	/**
	 * Registers a server offering a specified service on {@link ServiceFinder}
	 * 
	 * @param serviceName
	 * @param address
	 * @throws RemoteException
	 */
	public void register(String serviceName, String address, int rmiPort)
			throws RemoteException {
		serviceFinderStub.registerService(serviceName, PID, address, rmiPort);
	}

	/**
	 * Retreives the server detail of any one server offering a specified
	 * service.
	 * 
	 * @param serviceName
	 * @return
	 * @throws RemoteException
	 */
	public ServerDetail getServerDetails(String serviceName)
			throws RemoteException {
		return serviceFinderStub.getService(serviceName);
	}
	
	public ServerDetail getServerDetails(String serviceName, String requesterID)
			throws RemoteException {
		return serviceFinderStub.getService(serviceName, requesterID);
	}

	public ServerDetail getSpecificServerDetails(String serverName)
			throws RemoteException {
		return serviceFinderStub.getServerDetail(serverName);
	}

	
	/**
	 * Retreives the server details of all the servers offering a specified
	 * service.
	 * 
	 * @param serviceName
	 * @return
	 * @throws RemoteException
	 */
	public List<ServerDetail> getServersDetails(String serviceName)
			throws RemoteException {
		return serviceFinderStub.getServices(serviceName);
	}
	
	public void beat(String serverName) throws RemoteException {
		serviceFinderStub.beat(serverName);
	}
}
