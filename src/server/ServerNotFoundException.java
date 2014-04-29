package server;

public class ServerNotFoundException extends Exception {
	private static final long serialVersionUID = 4884789321663584120L;
	
	public ServerNotFoundException() {
	}

	public ServerNotFoundException(String message, Exception nestedException) {
		super(message, nestedException);
	}

	public ServerNotFoundException(String message) {
		super(message);
	}	
}
