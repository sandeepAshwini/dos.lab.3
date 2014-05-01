package server;

import java.io.IOException;
import java.rmi.ConnectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

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
	private static String OBELIX_SERVICE_NAME = "Obelix";

	private List<ServerDetail> services = new ArrayList<ServerDetail>();
	private Map<String, Long> heartbeats = new HashMap<String, Long>();
	private Map<String, Set<String>> clientStates = new HashMap<String, Set<String>>();
	private String currentMasterObelix = "";

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
			serviceFinderInstance.setupLoadBalancingThread();
		} catch (IOException e) {
			throw new OlympicException(
					"Registry Service could not be created.", e);
		}
	}

	private void setupLoadBalancingThread() {
		Thread thread = new Thread(new LoadBalancer(this.heartbeats,
				this.services, this.clientStates));
		thread.start();
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
			// registry = LocateRegistry.getRegistry(JAVA_RMI_PORT);
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
		ServerDetail newServerDetail = new ServerDetail(serviceName, PID,
				address, rmiPort);
		synchronized (this.services) {
			this.services.add(newServerDetail);
			synchronized (this.clientStates) {
				if (serviceName.compareTo(OBELIX_SERVICE_NAME) == 0) {
					this.clientStates.put(newServerDetail.getServerName(),
							new HashSet<String>());
				}
			}
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
		// System.out.println("Resolved " + serviceName + " to "
		// + pickedService.getServerName() + ".");
		return pickedService;
	}

	@Override
	public ServerDetail getService(String serviceName, String requesterID)
			throws RemoteException {
		ServerDetail pickedService = null;

		synchronized (this.services) {
			List<ServerDetail> matchingServices = getServices(serviceName);
			int num = random.nextInt(matchingServices.size());

			if (requesterID.startsWith("Cacophonix") || requesterID.startsWith("Obelix")) {
				synchronized (this.currentMasterObelix) {
					pickedService = Collections.max(matchingServices);
					if(!pickedService.getServerName().equals(currentMasterObelix)) {
						this.currentMasterObelix = pickedService.getServerName();
						notifyMasterObelix(pickedService, matchingServices);
					}
				}
			} else if (requesterID.startsWith("Client")) {
				synchronized (this.clientStates) {
					for (String instance : clientStates.keySet()) {
						if (clientStates.get(instance).contains(requesterID)) {
							pickedService = getServerDetail(instance);
							break;
						}
					}
					if (pickedService == null) {
						pickedService = matchingServices.get(num);
						Set<String> clients = this.clientStates
								.get(pickedService.getServerName());
						if (clients == null) {
							clients = new HashSet<String>();
							this.clientStates.put(
									pickedService.getServerName(), clients);
						}
						clients.add(requesterID);
					}
				}
			}
		}

		// System.out.println("Resolved " + serviceName + " to "
		// + pickedService.getServerName() + ".");
		return pickedService;
	}
	
	private void notifyMasterObelix(ServerDetail obelixMasterDetail, List<ServerDetail> obelixServersDetails) throws RemoteException {
		Registry registry = null;
		ObelixInterface obelixMasterStub = null;

		try {
			registry = LocateRegistry.getRegistry(
					obelixMasterDetail.getServiceAddress(),
					obelixMasterDetail.getServicePort());
			obelixMasterStub = (ObelixInterface) registry
					.lookup(obelixMasterDetail.getServerName());
		} catch (ConnectException e) {
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		
		if (obelixMasterStub != null) {
			obelixMasterStub.refreshCaches(obelixServersDetails);
		}

		return;
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

	public ServerDetail getServerDetail(String serverName)
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

	@Override
	public void beat(String serverName) throws RemoteException {
		// System.err.println("Received hearbeat from " + serverName);
		synchronized (this.heartbeats) {
			heartbeats.put(serverName, System.currentTimeMillis());
		}
	}
}

class LoadBalancer implements Runnable {

	private Map<String, Long> heartbeats;
	private List<ServerDetail> services;
	private Map<String, Set<String>> clientStates;
	private static int REBALANCING_INTERVAL = 1500;
	private static int HOLD_TIME = 3500;
	private static String OBELIX_SERVICE_NAME = "Obelix";

	public LoadBalancer(Map<String, Long> heartbeats,
			List<ServerDetail> services, Map<String, Set<String>> clientStates) {
		this.services = services;
		this.heartbeats = heartbeats;
		this.clientStates = clientStates;
	}

	private void removeStaleServers(Set<String> staleServerNames) {
		List<ServerDetail> staleServers = new ArrayList<ServerDetail>();
		synchronized (this.services) {
			System.out.println(staleServerNames);
			for (ServerDetail service : this.services) {
				if (staleServerNames.contains(service.getServerName())) {
					staleServers.add(service);
				}
			}
			for (ServerDetail staleServer : staleServers) {
				services.remove(staleServer);
			}
		}
	}

	@Override
	public void run() {
		Random random = new Random();
		while (true) {
			Set<String> staleServerNames = new HashSet<String>();
			Set<String> orphanClients = new HashSet<String>();

			synchronized (this.heartbeats) {
				for (String serverName : heartbeats.keySet()) {
					if (System.currentTimeMillis() - heartbeats.get(serverName) >= HOLD_TIME) {
						staleServerNames.add(serverName);
					}
				}
				for (String staleServerName : staleServerNames) {
					heartbeats.remove(staleServerName);
				}
			}
			
			removeStaleServers(staleServerNames);
			
			synchronized (this.clientStates) {
				for (String staleServerName : staleServerNames) {
					Set<String> removedClients = clientStates
							.remove(staleServerName);
					orphanClients.addAll(removedClients);
				}
				
				for (String orphanClient : orphanClients) {
					List<ServerDetail> obelixServers = new ArrayList<ServerDetail>();
					for (ServerDetail service : services) {
						if(service.getServiceName().compareTo(OBELIX_SERVICE_NAME) == 0) {
							obelixServers.add(service);
						}
					}
					ServerDetail pickedObelixService = obelixServers.get(random
							.nextInt(obelixServers.size()));
					Set<String> clients = this.clientStates
							.get(pickedObelixService.getServerName());
					clients.add(orphanClient);
				}

				System.out.println("Balancing...");
				List<String> clients = new ArrayList<String>();
				for (String instance : clientStates.keySet()) {
					clients.addAll(clientStates.get(instance));
					clientStates.put(instance, new HashSet<String>());
				}
				List<String> servers = new ArrayList<String>(
						clientStates.keySet());
				for (String client : clients) {
					String pickedServer = servers.get(random.nextInt(servers
							.size()));
					clientStates.get(pickedServer).add(client);
				}
				for (String server : servers) {
					System.out.println(clientStates.get(server).size());
				}
			}

			try {
				Thread.sleep(REBALANCING_INTERVAL);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}