package chordfinal;
import java.net.InetSocketAddress;
import java.util.Random;

public class FingersTable_setup extends Thread{

	private Chord_Node local;
	Random random;
	boolean alive;

	public FingersTable_setup (Chord_Node node) {
		local = node;
		alive = true;
		random = new Random();
	}

	@Override
	public void run() {
		while (alive) {
			int i = random.nextInt(31) + 2;
			InetSocketAddress ithfinger = local.find_successor(Utility.ithStart(local.getId(), i));
			local.updateFingers(i, ithfinger);
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
