package chordfinal;
import java.net.InetSocketAddress;
import java.util.HashMap;

public class Chord_Node {

	private long localId;
	private InetSocketAddress localAddress;
	private InetSocketAddress predecessor;
	private HashMap<Integer, InetSocketAddress> finger;

	private Socket_Listener listener;
	private Updator stabilize;
	private FingersTable_setup fix_fingers;
	private Predecessor_setup ask_predecessor;


	public long getLocalId() {
		return localId;
	}

	public void setLocalId(long localId) {
		this.localId = localId;
	}

	public InetSocketAddress getLocalAddress() {
		return localAddress;
	}

	public void setLocalAddress(InetSocketAddress localAddress) {
		this.localAddress = localAddress;
	}

	public HashMap<Integer, InetSocketAddress> getFinger() {
		return finger;
	}

	public void setFinger(HashMap<Integer, InetSocketAddress> finger) {
		this.finger = finger;
	}

	public Socket_Listener getListener() {
		return listener;
	}

	public void setListener(Socket_Listener listener) {
		this.listener = listener;
	}

	public Updator getStabilize() {
		return stabilize;
	}

	public void setStabilize(Updator stabilize) {
		this.stabilize = stabilize;
	}

	public FingersTable_setup getFix_fingers() {
		return fix_fingers;
	}

	public void setFix_fingers(FingersTable_setup fix_fingers) {
		this.fix_fingers = fix_fingers;
	}

	public Predecessor_setup getAsk_predecessor() {
		return ask_predecessor;
	}

	public void setAsk_predecessor(Predecessor_setup ask_predecessor) {
		this.ask_predecessor = ask_predecessor;
	}

	public Chord_Node (InetSocketAddress address) {

		localAddress = address;
		localId = Utility.hashSocketAddress(localAddress);

		// initialize an empty finge table
		finger = new HashMap<Integer, InetSocketAddress>();
		for (int i = 1; i <= 32; i++) {
			updateIthFinger (i, null);
		}

		// initialize predecessor
		predecessor = null;

		// initialize threads
		listener = new Socket_Listener(this);
		stabilize = new Updator(this);
		fix_fingers = new FingersTable_setup(this);
		ask_predecessor = new Predecessor_setup(this);
	}


	public void stopAllThreads() {
		if (listener != null)
			listener.toDie();
		if (fix_fingers != null)
			fix_fingers.toDie();
		if (stabilize != null)
			stabilize.toDie();
		if (ask_predecessor != null)
			ask_predecessor.toDie();
	}
	
	public boolean join (InetSocketAddress contact) {

		// if contact is other node (join ring), try to contact that node
		// (contact will never be null)
		if (contact != null && !contact.equals(localAddress)) {
			InetSocketAddress successor = Utility.requestAddress(contact, "FINDSUCC_" + localId);
			if (successor == null)  {
				System.out.println("\nCannot find node you are trying to contact. Please exit.\n");
				return false;
			}
			updateIthFinger(1, successor);
		}

		// start all threads	
		listener.start();
		stabilize.start();
		fix_fingers.start();
		ask_predecessor.start();

		return true;
	}

	public String notify(InetSocketAddress successor) {
		if (successor!=null && !successor.equals(localAddress))
			return Utility.sendRequest(successor, "IAMPRE_"+localAddress.getAddress().toString()+":"+localAddress.getPort());
		else
			return null;
	}

	public void notified (InetSocketAddress newpre) {
		if (predecessor == null || predecessor.equals(localAddress)) {
			this.setPredecessor(newpre);
		}
		else {
			long oldpre_id = Utility.hashSocketAddress(predecessor);
			long local_relative_id = Utility.computeRelativeId(localId, oldpre_id);
			long newpre_relative_id = Utility.computeRelativeId(Utility.hashSocketAddress(newpre), oldpre_id);
			if (newpre_relative_id > 0 && newpre_relative_id < local_relative_id)
				this.setPredecessor(newpre);
		}
	}



	private InetSocketAddress find_predecessor (long findid) {
		InetSocketAddress n = this.localAddress;
		InetSocketAddress n_successor = this.getSuccessor();
		InetSocketAddress most_recently_alive = this.localAddress;
		long n_successor_relative_id = 0;
		if (n_successor != null)
			n_successor_relative_id = Utility.computeRelativeId(Utility.hashSocketAddress(n_successor), Utility.hashSocketAddress(n));
		long findid_relative_id = Utility.computeRelativeId(findid, Utility.hashSocketAddress(n));

		while (!(findid_relative_id > 0 && findid_relative_id <= n_successor_relative_id)) {

			// temporarily save current node
			InetSocketAddress pre_n = n;

			// if current node is local node, find my closest
			if (n.equals(this.localAddress)) {
				n = this.closest_preceding_finger(findid);
			}

			// else current node is remote node, sent request to it for its closest
			else {
				InetSocketAddress result = Utility.requestAddress(n, "CLOSEST_" + findid);

				// if fail to get response, set n to most recently 
				if (result == null) {
					n = most_recently_alive;
					n_successor = Utility.requestAddress(n, "YOURSUCC");
					if (n_successor==null) {
						System.out.println("It's not possible.");
						return localAddress;
					}
					continue;
				}

				// if n's closest is itself, return n
				else if (result.equals(n))
					return result;

				// else n's closest is other node "result"
				else {	
					// set n as most recently alive
					most_recently_alive = n;		
					// ask "result" for its successor
					n_successor = Utility.requestAddress(result, "YOURSUCC");	
					// if we can get its response, then "result" must be our next n
					if (n_successor!=null) {
						n = result;
					}
					// else n sticks, ask n's successor
					else {
						n_successor = Utility.requestAddress(n, "YOURSUCC");
					}
				}

				// compute relative ids for while loop judgement
				n_successor_relative_id = Utility.computeRelativeId(Utility.hashSocketAddress(n_successor), Utility.hashSocketAddress(n));
				findid_relative_id = Utility.computeRelativeId(findid, Utility.hashSocketAddress(n));
			}
			if (pre_n.equals(n))
				break;
		}
		return n;
	}

	public InetSocketAddress find_successor (long id) {

		// initialize return value as this node's successor (might be null)
		InetSocketAddress ret = this.getSuccessor();

		// find predecessor
		InetSocketAddress pre = find_predecessor(id);

		// if other node found, ask it for its successor
		if (!pre.equals(localAddress))
			ret = Utility.requestAddress(pre, "YOURSUCC");

		// if ret is still null, set it as local node, return
		if (ret == null)
			ret = localAddress;

		return ret;
	}
	
	public InetSocketAddress closest_preceding_finger (long findid) {
		
		long findid_relative = Utility.computeRelativeId(findid, localId);

		// check from last item in finger table
		for (int i = 32; i > 0; i--) {
			InetSocketAddress ith_finger = finger.get(i);
			if (ith_finger == null) {
				continue;
			}
			long ith_finger_id = Utility.hashSocketAddress(ith_finger);
			long ith_finger_relative_id = Utility.computeRelativeId(ith_finger_id, localId);

			// if its relative id is the closest, check if its alive
			if (ith_finger_relative_id > 0 && ith_finger_relative_id < findid_relative)  {
				String response  = Utility.sendRequest(ith_finger, "KEEP");

				//it is alive, return it
				if (response!=null &&  response.equals("ALIVE")) {
					return ith_finger;
				}

				// else, remove its existence from finger table
				else {
					updateFingers(-2, ith_finger);
				}
			}
		}
		return localAddress;
	}

	//using threads here!!!!!!!!!!!
	public synchronized void updateFingers(int i, InetSocketAddress value) {

		// valid index in [1, 32], just update the ith finger
		if (i > 0 && i <= 32) {
			updateIthFinger(i, value);
		}

		// caller wants to delete
		else if (i == -1) {
			deleteSuccessor();
		}

		// caller wants to delete a finger in table
		else if (i == -2) {
			deleteCertainFinger(value);

		}

		// caller wants to fill successor
		else if (i == -3) {
			fillSuccessor();
		}

	}

	private void updateIthFinger(int i, InetSocketAddress value) {
		finger.put(i, value);

		// if the updated one is successor, notify the new successor
		if (i == 1 && value != null && !value.equals(localAddress)) {
			notify(value);
		}
	}

	private void deleteSuccessor() {
		InetSocketAddress successor = getSuccessor();

		//nothing to delete, just return
		if (successor == null)
			return;

		// find the last existence of successor in the finger table
		int i = 32;
		for (i = 32; i > 0; i--) {
			InetSocketAddress ithfinger = finger.get(i);
			if (ithfinger != null && ithfinger.equals(successor))
				break;
		}

		// delete it, from the last existence to the first one
		for (int j = i; j >= 1 ; j--) {
			updateIthFinger(j, null);
		}

		// if predecessor is successor, delete it
		if (predecessor!= null && predecessor.equals(successor))
			setPredecessor(null);

		// try to fill successor
		fillSuccessor();
		successor = getSuccessor();

		// if successor is still null or local node, 
		// and the predecessor is another node, keep asking 
		// it's predecessor until find local node's new successor
		if ((successor == null || successor.equals(successor)) && predecessor!=null && !predecessor.equals(localAddress)) {
			InetSocketAddress p = predecessor;
			InetSocketAddress p_pre = null;
			while (true) {
				p_pre = Utility.requestAddress(p, "YOURPRE");
				if (p_pre == null)
					break;

				// if p's predecessor is node is just deleted, 
				// or itself (nothing found in p), or local address,
				// p is current node's new successor, break
				if (p_pre.equals(p) || p_pre.equals(localAddress)|| p_pre.equals(successor)) {
					break;
				}

				// else, keep asking
				else {
					p = p_pre;
				}
			}

			// update successor
			updateIthFinger(1, p);
		}
	}

	private void deleteCertainFinger(InetSocketAddress f) {
		for (int i = 32; i > 0; i--) {
			InetSocketAddress ithfinger = finger.get(i);
			if (ithfinger != null && ithfinger.equals(f))
				finger.put(i, null);
		}
	}

	private void fillSuccessor() {
		InetSocketAddress successor = this.getSuccessor();
		if (successor == null || successor.equals(localAddress)) {
			for (int i = 2; i <= 32; i++) {
				InetSocketAddress ithfinger = finger.get(i);
				if (ithfinger!=null && !ithfinger.equals(localAddress)) {
					for (int j = i-1; j >=1; j--) {
						updateIthFinger(j, ithfinger);
					}
					break;
				}
			}
		}
		successor = getSuccessor();
		if ((successor == null || successor.equals(localAddress)) && predecessor!=null && !predecessor.equals(localAddress)) {
			updateIthFinger(1, predecessor);
		}

	}

	public void clearPredecessor () {
		setPredecessor(null);
	}

	/**
	 * Set predecessor using a new value.
	 * @param pre
	 */
	private synchronized void setPredecessor(InetSocketAddress pre) {
		predecessor = pre;
	}

	public long getId() {
		return localId;
	}

	public InetSocketAddress getAddress() {
		return localAddress;
	}

	public InetSocketAddress getPredecessor() {
		return predecessor;
	}

	public InetSocketAddress getSuccessor() {
		if (finger != null && finger.size() > 0) {
			return finger.get(1);
		}
		return null;
	}

	public void printNeighbors () {
		System.out.println("\nYou are listening on port "+localAddress.getPort()+"."
				+ "\nYour position is "+Utility.hexIdAndPosition(localAddress)+".");
		InetSocketAddress successor = finger.get(1);
		
		// if it cannot find both predecessor and successor
		if ((predecessor == null || predecessor.equals(localAddress)) && (successor == null || successor.equals(localAddress))) {
			System.out.println("Your predecessor is yourself.");
			System.out.println("Your successor is yourself.");

		}
		
		// else, it can find either predecessor or successor
		else {
			if (predecessor != null) {
				System.out.println("Your predecessor is node "+predecessor.getAddress().toString()+", "
						+ "port "+predecessor.getPort()+ ", position "+Utility.hexIdAndPosition(predecessor)+".");
			}
			else {
				System.out.println("Your predecessor is updating.");
			}

			if (successor != null) {
				System.out.println("Your successor is node "+successor.getAddress().toString()+", "
						+ "port "+successor.getPort()+ ", position "+Utility.hexIdAndPosition(successor)+".");
			}
			else {
				System.out.println("Your successor is updating.");
			}
		}
	}

	public void printDataStructure () {
		System.out.println("\n----------------------------------------------------------------------------------------");
		System.out.println("\nLOCAL:\t\t\t\t"+localAddress.toString()+"\t"+Utility.hexIdAndPosition(localAddress));
		if (predecessor != null)
			System.out.println("\nPREDECESSOR:\t\t\t"+predecessor.toString()+"\t"+Utility.hexIdAndPosition(predecessor));
		else 
			System.out.println("\nPREDECESSOR:\t\t\tNULL");
		System.out.println("\nFINGER TABLE:\n");
		for (int i = 1; i <= 32; i++) {
			long ithstart = Utility.ithStart(Utility.hashSocketAddress(localAddress),i);
			InetSocketAddress f = finger.get(i);
			StringBuilder sb = new StringBuilder();
			sb.append(i+"\t"+ Utility.longTo8DigitHex(ithstart)+"\t\t");
			if (f!= null)
				sb.append(f.toString()+"\t"+Utility.hexIdAndPosition(f));

			else 
				sb.append("NULL");
			System.out.println(sb.toString());
		}
		System.out.println("\n-----------------------------------------------------------------------------------------\n");
	}


}
