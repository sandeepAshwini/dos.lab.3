package util;

import java.io.Serializable;

/**
 * Describes a server offering a specified service with a server name and the
 * address on which it is hosted.
 * 
 * @author aravind
 * 
 */
public class ServerDetail implements Serializable, Comparable<ServerDetail> {
	private static final long serialVersionUID = -7853196643095509724L;
	private String serviceName;
	private int PID;
	private String serviceAddress;
	private int servicePort;

	public ServerDetail(String serviceName, int PID, String serviceAddress,
			int servicePort) {
		this.serviceName = serviceName;
		this.PID = PID;
		this.serviceAddress = serviceAddress;
		this.servicePort = servicePort;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public int getPID() {
		return PID;
	}

	public void setPID(int PID) {
		this.PID = PID;
	}

	public String getServiceAddress() {
		return serviceAddress;
	}

	public void setServiceAddress(String serviceAddress) {
		this.serviceAddress = serviceAddress;
	}

	public String getServerName() {
		return this.serviceName + this.PID;
	}

	public int getServicePort() {
		return servicePort;
	}

	public void setServicePort(int servicePort) {
		this.servicePort = servicePort;
	}

	@Override
	public int compareTo(ServerDetail that) {
		String[] thisOctets = this.serviceAddress.split("\\.");
		String[] thatOctets = that.serviceAddress.split("\\.");
		for (int i = 0; i < thisOctets.length; i++) {
			if (Integer.parseInt(thisOctets[i]) - Integer.parseInt(thatOctets[i]) == 0) {
				continue;
			}
			return (Integer.parseInt(thisOctets[i]) - Integer.parseInt(thatOctets[i]));
		}
		return this.servicePort - that.servicePort;
	}
}
