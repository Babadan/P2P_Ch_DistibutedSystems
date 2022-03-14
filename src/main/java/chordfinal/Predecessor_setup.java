package chordfinal;
import java.net.InetSocketAddress;

public class Predecessor_setup extends Thread {
	
	private Chord_Node local;
	private boolean alive;
	
	public Chord_Node getLocal() {
		return local;
	}

	public void setLocal(Chord_Node local) {
		this.local = local;
	}


	public void setAlive(boolean alive) {
		this.alive = alive;
	}

	public Predecessor_setup(Chord_Node _local) {
		local = _local;
		alive = true;
	}
	
	@Override
	public void run() {
		while (alive) {
			InetSocketAddress predecessor = local.getPredecessor();
			if (predecessor != null) {
				String response = Utility.sendRequest(predecessor, "KEEP");
				if (response == null || !response.equals("ALIVE")) {
					local.clearPredecessor();	
				}

			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void toDie() {
		alive = false;
	}
}


