package com.talkmuch;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.*;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class P2PChat {
    private static int UDP_PORT = 9999;
    private static int TCP_PORT = 8888;
    private static final String DISCOVERY_MESSAGE = "P2P_CHAT_DISCOVER";
    
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

        if (!startTcpServer()) {
            System.err.println("Fatal: Could not bind TCP server to any port.");
            return;
        }

        startUdpListener();

        new Thread(P2PChat::runUdpBroadcaster, "UDP-Broadcaster").start();

        runConsoleReader();
    }

    private static boolean startTcpServer() {
        try {
            ServerSocket serverSocket = new ServerSocket(TCP_PORT);
            new Thread(() -> runTcpServer(serverSocket), "TCP-Server").start();
            return true;
        } catch (IOException e) {
            try {
                ServerSocket serverSocket = new ServerSocket(0);
                TCP_PORT = serverSocket.getLocalPort();
                System.out.println("[System] Default TCP port busy. Allocated dynamic port: " + TCP_PORT);
                new Thread(() -> runTcpServer(serverSocket), "TCP-Server").start();
                return true;
            } catch (IOException ex) {
                return false;
            }
        }
    }

    private static void runTcpServer(ServerSocket serverSocket) {
        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                if (addPeer(clientSocket)) {
                    new Thread(() -> handlePeerMessages(clientSocket), "Peer-Handler-" + clientSocket.getRemoteSocketAddress()).start();
                }
            }
        } catch (IOException e) {
            System.err.println("TCP Server run error: " + e.getMessage());
        }
    }

    private static void startUdpListener() {
        new Thread(() -> {
            DatagramSocket udpSocket = null;
            try {
                udpSocket = new DatagramSocket(UDP_PORT);
            } catch (SocketException e) {
                try {
                    udpSocket = new DatagramSocket(0);
                    UDP_PORT = udpSocket.getLocalPort();
                    System.out.println("[System] Default UDP port busy. Allocated dynamic port: " + UDP_PORT);
                } catch (SocketException ex) {
                    System.err.println("UDP Listener critical setup error: " + ex.getMessage());
                    return;
                }
            }

            try {
                udpSocket.setBroadcast(true);
                byte[] buffer = new byte[1024];

                while (true) {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    udpSocket.receive(packet);

                    String rawMessage = new String(packet.getData(), 0, packet.getLength()).trim();
                    InetAddress peerIp = packet.getAddress();

                    if (isLocalAddress(peerIp) && packet.getPort() == UDP_PORT) {
                        continue; 
                    }

                    if (rawMessage.startsWith(DISCOVERY_MESSAGE + ":")) {
                        try {
                            int remoteTcpPort = Integer.parseInt(rawMessage.split(":")[1]);
                            tryToConnectToPeer(peerIp, remoteTcpPort);
                        } catch (Exception ignored) {}
                    }
                }
            } catch (IOException e) {
                System.err.println("UDP Listener execution error: " + e.getMessage());
            } finally {
                if (udpSocket != null) udpSocket.close();
            }
        }, "UDP-Listener").start();
    }

    private static void runUdpBroadcaster() {
        try (DatagramSocket udpSocket = new DatagramSocket()) {
            udpSocket.setBroadcast(true);
            
            while (true) {
                String dynamicPayload = DISCOVERY_MESSAGE + ":" + TCP_PORT;
                byte[] buffer = dynamicPayload.getBytes();
                
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length, InetAddress.getByName("255.255.255.255"), 9999);
                udpSocket.send(packet);
                
                if (UDP_PORT != 9999) {
                    DatagramPacket localFallbackPacket = new DatagramPacket(buffer, buffer.length, InetAddress.getByName("255.255.255.255"), UDP_PORT);
                    udpSocket.send(localFallbackPacket);
                }
                
                Thread.sleep(3000); 
            }
        } catch (IOException e) {
            System.err.println("UDP Broadcaster error: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

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

    private static synchronized void tryToConnectToPeer(InetAddress ip, int remoteTcpPort) {
        if (remoteTcpPort == TCP_PORT && (ip.isLoopbackAddress() || isLocalAddress(ip))) {
            return;
        }

        for (Socket s : peerSockets) {
            if (s.getInetAddress().equals(ip) && s.getPort() == remoteTcpPort && !s.isClosed()) return;
        }

        try {
            Socket socket = new Socket(ip, remoteTcpPort);
            if (addPeer(socket)) {
                new Thread(() -> handlePeerMessages(socket), "Peer-Handler-" + ip).start();
            }
        } catch (IOException e) {}
    }

    private static synchronized boolean addPeer(Socket socket) {
        InetAddress remoteIp = socket.getInetAddress();
        int remotePort = socket.getPort();

        if (remotePort == TCP_PORT && (remoteIp.isLoopbackAddress() || isLocalAddress(remoteIp))) {
            try { socket.close(); } catch (IOException ignored) {}
            return false;
        }

        for (Socket s : peerSockets) {
            if (s.getInetAddress().equals(remoteIp) && !s.isClosed()) {
                if (s.getPort() == remotePort || s.getLocalPort() == remotePort) {
                    try { socket.close(); } catch (IOException ignored) {}
                    return false;
                }
            }
        }

        peerSockets.add(socket);
        System.out.println("\n[System] Connected to peer: " + remoteIp.getHostAddress() + ":" + remotePort);
        return true;
    }

    private static void handlePeerMessages(Socket socket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            String incoming;
            while ((incoming = in.readLine()) != null) {
                System.out.println(incoming);
            }
        } catch (IOException e) {}
          finally {
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
            } catch (IOException e) {}
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
