package chordfinal;
import java.net.InetSocketAddress;

public class Updator extends Thread {
	
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

	public Updator(Chord_Node _local) {
		local = _local;
		alive = true;
	}

	@Override
	public void run() {
		while (alive) {
			InetSocketAddress successor = local.getSuccessor();
			if (successor == null || successor.equals(local.getAddress())) {
				local.updateFingers(-3, null); //fill
			}
			successor = local.getSuccessor();
			if (successor != null && !successor.equals(local.getAddress())) {

				// try to get my successor's predecessor
				InetSocketAddress x = Utility.requestAddress(successor, "YOURPRE");

				// if bad connection with successor! delete successor
				if (x == null) {
					local.updateFingers(-1, null);
				}

				// else if successor's predecessor is not itself
				else if (!x.equals(successor)) {
					long local_id = Utility.hashSocketAddress(local.getAddress());
					long successor_relative_id = Utility.computeRelativeId(Utility.hashSocketAddress(successor), local_id);
					long x_relative_id = Utility.computeRelativeId(Utility.hashSocketAddress(x),local_id);
					if (x_relative_id>0 && x_relative_id < successor_relative_id) {
						local.updateFingers(1,x);
					}
				}
				
				// successor's predecessor is successor itself, then notify successor
				else {
					local.notify(successor);
				}
			}

			try {
				Thread.sleep(60);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	public void toDie() {
		alive = false;
	}





}
