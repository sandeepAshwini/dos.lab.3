package server;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import util.ServerDetail;

public interface ServiceFinderInterface extends Remote {
	public void registerService(String serviceName, int PID, String address,
			int servicePort) throws RemoteException;

	public ServerDetail getService(String serviceName) throws RemoteException;

	public List<ServerDetail> getServices(String serviceName)
			throws RemoteException;
	
	public ServerDetail getService(String serviceName, String requesterID) throws RemoteException;

	public ServerDetail getServerDetail(String serverName) throws RemoteException;

	public void beat(String serverName) throws RemoteException;
}
