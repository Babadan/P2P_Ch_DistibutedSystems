package chordfinal;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Scanner;

public class Main {

    private static Chord_Node m_node;
    private static InetSocketAddress m_contact;
    private static Utility m_helper;


    public static Chord_Node getM_node() {
        return m_node;
    }

    public static void setM_node(Chord_Node m_node) {
        Main.m_node = m_node;
    }

    public static InetSocketAddress getM_contact() {
        return m_contact;
    }

    public static void setM_contact(InetSocketAddress m_contact) {
        Main.m_contact = m_contact;
    }

    public static Utility getM_helper() {
        return m_helper;
    }

    public static void setM_helper(Utility m_helper) {
        Main.m_helper = m_helper;
    }

    public static void main (String[] args) {

        m_helper = new Utility();

        // get local machine's ip
        String local_ip = null;
        try {
            local_ip = InetAddress.getLocalHost().getHostAddress();

            local_ip = "192.168.40.1";
            //local_ip = "192.168.40.2";
            //local_ip = "192.168.40.3";

        } catch (UnknownHostException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        // create node
        m_node = new Chord_Node (Utility.createSocketAddress(local_ip+":"+args[0]));

        // determine if it's creating or joining a existing ring
        // create, contact is this node itself
        if (args.length == 1) {
            m_contact = m_node.getAddress();
        }

        // join, contact is another node
        else if (args.length == 3) {
            m_contact = Utility.createSocketAddress(args[1]+":"+args[2]);
            if (m_contact == null) {
                System.out.println("Cannot find address you are trying to contact. Now exit.");
                return;
            }
        }

        else {
            System.out.println("Wrong input. Now exit.");
            System.exit(0);
        }

        // try to join ring from contact node
        boolean successful_join = m_node.join(m_contact);

        // fail to join contact node
        if (!successful_join) {
            System.out.println("Cannot connect with node you are trying to contact. Now exit.");
            System.exit(0);
        }

        // print join info
        System.out.println("Joining the Chord ring.");
        System.out.println("Local IP: "+local_ip);
        m_node.printNeighbors();

        // begin to take user input, "info" or "quit"
        Scanner userinput = new Scanner(System.in);
        while(true) {
            System.out.println("\nType \"info\" to check this node's data or \n type \"quit\"to leave ring: ");
            String command = null;
            command = userinput.next();
            if (command.startsWith("quit")) {
                m_node.stopAllThreads();
                System.out.println("Leaving the ring...");
                System.exit(0);

            }
            else if (command.startsWith("info")) {
                m_node.printDataStructure();
            }
        }
    }


}