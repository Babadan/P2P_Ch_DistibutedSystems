package chordfinal;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Socket_Listener extends Thread {

	private Chord_Node local;
	private ServerSocket serverSocket;
	private boolean alive;

	public Chord_Node getLocal() {
		return local;
	}

	public void setLocal(Chord_Node local) {
		this.local = local;
	}

	public ServerSocket getServerSocket() {
		return serverSocket;
	}

	public void setServerSocket(ServerSocket serverSocket) {
		this.serverSocket = serverSocket;
	}

	public void setAlive(boolean alive) {
		this.alive = alive;
	}

	public Socket_Listener (Chord_Node n) {
		local = n;
		alive = true;
		InetSocketAddress localAddress = local.getAddress();
		int port = localAddress.getPort();

		//open server/listener socket
		try {
			serverSocket = new ServerSocket(port);
		} catch (IOException e) {
			throw new RuntimeException("\nCannot open listener port "+port+". Now exit.\n", e);
		}
	}

	@Override
	public void run() {
		while (alive) {
			Socket talkSocket = null;
			try {
				talkSocket = serverSocket.accept();
			} catch (IOException e) {
				throw new RuntimeException(
						"Cannot accepting connection", e);
			}

			//new talker
			new Thread(new Request_Handler(talkSocket, local)).start();
		}
	}

	public void toDie() {
		alive = false;
	}
}
