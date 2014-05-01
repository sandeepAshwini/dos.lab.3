package util;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Enumeration;

import base.OlympicException;

/**
 * A util class to create a registry service with the required configuration
 * parameters to be used by the servers.
 * 
 * @author aravind
 * 
 */
public class RegistryService {
	private static int PROPAGATION_INTERVAL = 0;
	//private static String RMI_COMMAND = "rmiregistry";
	//private static String USE_CODEBASE_ONLY_FALSE = "-J-Djava.rmi.server.useCodebaseOnly=false";

	public RegistryService() {
	}

	/**
	 * Creates the registry service.
	 * 
	 * @throws IOException
	 */
	private Registry startRegistryService(int rmiPort) throws IOException {
		return LocateRegistry.createRegistry(rmiPort);
		// Runtime.getRuntime().exec(
		// new String[] { RMI_COMMAND, USE_CODEBASE_ONLY_FALSE,
		// (new Integer(rmiPort)).toString() });
	}

	/**
	 * Returns the localIPAddress to be used to configure servers and clients.
	 * 
	 * @return String
	 * @throws SocketException
	 */
	public String getLocalIPAddress() throws SocketException {
		Enumeration<NetworkInterface> networkInterfaces = NetworkInterface
				.getNetworkInterfaces();
		while (networkInterfaces.hasMoreElements()) {
			NetworkInterface networkInterface = (NetworkInterface) networkInterfaces
					.nextElement();
			Enumeration<InetAddress> networkInterfaceAddresses = networkInterface
					.getInetAddresses();
			while (networkInterfaceAddresses.hasMoreElements()) {
				InetAddress networkInterfaceAddress = (InetAddress) networkInterfaceAddresses
						.nextElement();
				if (!networkInterfaceAddress.isLoopbackAddress()
						&& !(networkInterfaceAddress instanceof Inet6Address)) {
					return networkInterfaceAddress.getHostAddress();
				}
			}
		}
		return null;
	}

	/**
	 * Sets up the local registry service.
	 * 
	 * @throws OlympicException
	 */
	public Registry setupLocalRegistry(int rmiPort) throws OlympicException {
		Registry newRegistry = null;
		try {
			String ipAddress = this.getLocalIPAddress();
			newRegistry = this.startRegistryService(rmiPort);
			System.err.println("Registry Service started at : " + ipAddress
					+ ":" + rmiPort + ".");
			Thread.sleep(PROPAGATION_INTERVAL);
		} catch (IOException e) {
			throw new OlympicException(
					"Registry service could not be instantiated.");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return newRegistry;
	}
}
