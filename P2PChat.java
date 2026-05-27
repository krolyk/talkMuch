import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.*;
import java.util.Collections;
import java.util.Set;
import java.util.ConcurrentModificationException;
import java.util.concurrent.ConcurrentHashMap;

public class P2PChat {
    private static final int UDP_PORT = 9999;
    private static final int TCP_PORT = 8888;
    private static final String DISCOVERY_MESSAGE = "P2P_CHAT_DISCOVER";
    
    // Thread-safe set to hold active peer TCP sockets
    private static final Set<Socket> peerSockets = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static String username = "Unknown";

    public static void main(String[] args) {
        System.out.print("Enter your username: ");
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            username = reader.readLine();
            if (username == null || username.trim().isEmpty()) username = "Peer";
        } catch (IOException e) {
            username = "Peer";
        }

        System.out.println("--- Starting P2P Chat Instance [" + username + "] ---");

        // 1. Start TCP Server to handle incoming chat connections
        new Thread(P2PChat::runTcpServer, "TCP-Server").start();

        // 2. Start UDP Listener to detect other peers automatically
        new Thread(P2PChat::runUdpListener, "UDP-Listener").start();

        // 3. Start UDP Broadcaster to let other peers find us
        new Thread(P2PChat::runUdpBroadcaster, "UDP-Broadcaster").start();

        // 4. Handle user input in the main thread
        runConsoleReader();
    }

    // --- 1. TCP SERVER ---
    private static void runTcpServer() {
        try (ServerSocket serverSocket = new ServerSocket(TCP_PORT)) {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                if (addPeer(clientSocket)) {
                    new Thread(() -> handlePeerMessages(clientSocket), "Peer-Handler-" + clientSocket.getRemoteSocketAddress()).start();
                }
            }
        } catch (IOException e) {
            System.err.println("TCP Server error: " + e.getMessage());
        }
    }

    // --- 2. UDP LISTENER ---
    private static void runUdpListener() {
        try (DatagramSocket udpSocket = new DatagramSocket(UDP_PORT)) {
            udpSocket.setBroadcast(true);
            byte[] buffer = new byte[1024];

            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                udpSocket.receive(packet);

                String message = new String(packet.getData(), 0, packet.getLength()).trim();
                InetAddress peerIp = packet.getAddress();

                // Ignore packets sent by ourselves
                if (isLocalAddress(peerIp)) continue;

                if (DISCOVERY_MESSAGE.equals(message)) {
                    // Discovered a peer! Attempt a TCP connection out to them
                    tryToConnectToPeer(peerIp);
                }
            }
        } catch (IOException e) {
            System.err.println("UDP Listener error: " + e.getMessage());
        }
    }

    // --- 3. UDP BROADCASTER ---
    private static void runUdpBroadcaster() {
        try (DatagramSocket udpSocket = new DatagramSocket()) {
            udpSocket.setBroadcast(true);
            byte[] buffer = DISCOVERY_MESSAGE.getBytes();
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, InetAddress.getByName("255.255.255.255"), UDP_PORT);

            while (true) {
                udpSocket.send(packet);
                Thread.sleep(5000); // Broadcast presence every 5 seconds
            }
        } catch (IOException e) {
            System.err.println("UDP Broadcaster error: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // --- 4. CONSOLE WRITER ---
    private static void runConsoleReader() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("System ready. Start typing your messages below:\n");

        while (true) {
            try {
                String text = reader.readLine();
                if (text == null) break;
                if (text.trim().isEmpty()) continue;

                String formattedMessage = "[" + username + "]: " + text;
                broadcastMessage(formattedMessage);
            } catch (IOException e) {
                System.err.println("Console read error: " + e.getMessage());
            }
        }
    }

    // --- HELPER METHODS ---
    private static synchronized void tryToConnectToPeer(InetAddress ip) {
        // Check if we are already connected to this IP to avoid duplicate sockets
        for (Socket s : peerSockets) {
            if (s.getInetAddress().equals(ip)) return;
        }

        try {
            Socket socket = new Socket(ip, TCP_PORT);
            if (addPeer(socket)) {
                new Thread(() -> handlePeerMessages(socket), "Peer-Handler-" + ip).start();
            }
        } catch (IOException e) {
            // Quietly fail; the peer might be connecting to us simultaneously
        }
    }

    private static synchronized boolean addPeer(Socket socket) {
        // Double-check to maintain a clean structure
        for (Socket s : peerSockets) {
            if (s.getInetAddress().equals(socket.getInetAddress()) && !s.isClosed()) {
                try { socket.close(); } catch (IOException ignored) {}
                return false;
            }
        }
        peerSockets.add(socket);
        System.out.println("\n[System] Connected to a new peer at: " + socket.getInetAddress().getHostAddress());
        return true;
    }

    private static void handlePeerMessages(Socket socket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            String incoming;
            while ((incoming = in.readLine()) != null) {
                System.out.println(incoming);
            }
        } catch (IOException e) {
            // Connection lost
        } finally {
            peerSockets.remove(socket);
            System.out.println("\n[System] Peer disconnected: " + socket.getInetAddress().getHostAddress());
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private static void broadcastMessage(String message) {
        for (Socket socket : peerSockets) {
            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                out.println(message);
            } catch (IOException e) {
                // Stale connection handler will clean this up
            }
        }
    }

    private static boolean isLocalAddress(InetAddress addr) {
        if (addr.isLoopbackAddress()) return true;
        try {
            return NetworkInterface.getByInetAddress(addr) != null;
        } catch (SocketException e) {
            return false;
        }
    }
}
