
import java.io.*;
import java.net.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author David Morales
 */
public class RingNode {

    String nextNodeIp;
    int nextPort;
    int listenPort;
    ServerSocket listenSocket;
    Socket next;
    Socket before;
    boolean beforeConnected;
    boolean nextConnected;
    HashMap<String, String> msg_map;
    boolean serverFunction;
    boolean closeRing;
    ArrayList<String> msg_queue;

    public static void main(String[] args) {
        new RingNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]), args[2]);
    }

    public String getMyIp() {
        try {
            Enumeration<NetworkInterface> n = NetworkInterface.getNetworkInterfaces();
            while (n.hasMoreElements()) {
                NetworkInterface e = n.nextElement();

                Enumeration<InetAddress> a = e.getInetAddresses();
                while (a.hasMoreElements()) {
                    InetAddress addr = a.nextElement();
                    if (addr.getHostAddress().matches("\\d*\\.\\d*\\.\\d*\\.\\d*") && !addr.getHostAddress().equals("127.0.0.1")) {
                        return addr.getHostAddress();
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return "";
    }

    public RingNode(int listenPort, int nextPort, String nextIp) {

//        System.out.println(getMyIp());
        beforeConnected = false;
        nextConnected = false;
        serverFunction = false;
        this.listenPort = listenPort;
        this.nextPort = nextPort;
        this.nextNodeIp = nextIp;
        this.msg_map = new HashMap<String, String>();
        msg_queue = new ArrayList<String>();
        tryConnect(false);
        try {
            listenSocket = new ServerSocket(listenPort);
        } catch (IOException ex) {
            System.out.printf("El puerto %d esta siendo usado,", listenPort);
        }
        listen();
    }

    public void sendToNext(String msg) {

        DataOutputStream out;
        try {
            out = new DataOutputStream(next.getOutputStream());
            out.writeUTF(msg);
        } catch (IOException ex) {
        }

    }

    public void sendToBefore(String msg) {

        DataOutputStream out;
        try {
            out = new DataOutputStream(before.getOutputStream());
            out.writeUTF(msg);
        } catch (IOException ex) {
        }

    }

    public void serverFunction() {
        Iterator it = msg_map.entrySet().iterator();
        try {
            RandomAccessFile rf = new RandomAccessFile("informacion_recibida.txt", "rw");

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                rf.writeUTF(pair.getValue().toString()+"\n");
                it.remove();
            }
            rf.close();
        } catch (Exception ex) {
        }
    }

    public void receiveFromBefore() {

        Thread receive = new Thread(new Runnable() {

            @Override
            public void run() {
                String myIP = getMyIp();
                while (beforeConnected) {

                    try {
                        DataInputStream in = new DataInputStream(before.getInputStream());
                        String msg = in.readUTF();
                        String data[] = msg.split(";");
                        if (data[0].equals("reconnect")) {
                            if (next != null) {
                                if (serverFunction) {

                                    sendToNext("send_msg");
                                    sendToNext("msg;" + getMyInfo());
                                } else {
                                    sendToNext(msg);
                                }

                            }
                        }

                        if (data[0].equals("send_msg") && !serverFunction) {
                            sendToNext("msg;" + getMyInfo());
                        }

                        if (data[0].equals("is_closed") && !serverFunction) {

                            if (data[1].equals(myIP)) {
                                //soy servidor
                                serverFunction = true;
                                System.out.println("Se establece como servidor");

                                sendToBefore("set_server;" + getMyIp());
                                sendToBefore("msg;" + getMyInfo());

                            } else {
                                msg_queue.add(0, msg);
                            }
                            if (!serverFunction && next != null) {
                                sendToNext(msg);
                            }

                        }


                        if (data[0].equals("msg")) {
                            //System.out.println(msg);
                            if (serverFunction) {
                                msg_map.put(data[1], msg);

                                if (data[1].equals(myIP)) {
                                    // el mensaje ya dio la vuelta es decir se completo el envio en anillo y debe guardar el archivo
                                    serverFunction();
                                }
                            } else {
                                sendToNext(msg);
                            }



                        } else if (data[0].equals("set_last")) {
                            if (next != null) {
                                sendToNext(msg);
                            } else {
                                // recibio la informacion de quien es el ultimo entoncs reconecta el anillo
                                nextNodeIp = data[1];
                                nextPort = Integer.parseInt(data[2]);
                                System.out.println("Reconectando con " + nextNodeIp);
                                tryConnect(true);

                            }
                        }

                    } catch (IOException ex) {
                        System.out.println("Se desconecto el nodo anterior");
                        before = null;
                        beforeConnected = false;
                        listen();

                    }

                }
            }
        });
        receive.start();
    }

    public void receiveFromNext() {
        Thread receive = new Thread(new Runnable() {

            @Override
            public void run() {
                while (nextConnected) {
                    try {
                        DataInputStream in = new DataInputStream(next.getInputStream());
                        String msg = in.readUTF();
                        String data[] = msg.split(";");
                        if (data[0].equals("get_last")) {
                            if (before != null) {
                                sendToBefore(msg);
                            } else {
                                sendToNext("set_last;" + getMyIp() + ";" + listenPort);

                            }
                        } else if (data[0].equals("set_server") && !serverFunction) {
                            if (!serverFunction) {
                                System.out.println("El servidor es: " + data[1]);
                                sendToBefore(msg);
                                sendToBefore("msg;" + getMyInfo());
                            }

                        }
                        if (data[0].equals("msg")) {
                            //System.out.println(msg);
                            if (serverFunction) {
                                msg_map.put(data[1], msg);

                                if (data[1].equals(getMyIp())) {
                                    // el mensaje ya dio la vuelta es decir se completo el envio en anillo y debe guardar el archivo
                                    serverFunction();
                                }
                            } else {
                                sendToBefore(msg);
                            }



                        }

                    } catch (IOException ex) {
                        System.out.println("Se desconecto el nodo siguiente");
                        next = null;
                        nextConnected = false;
                        sendToBefore("get_last");
                    }
                }
            }
        });
        receive.start();
    }

    public String getMyInfo() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        return getMyIp() + ";" + dateFormat.format(date);
    }

    public void tryConnect(boolean reconnect) {
        final boolean rec = reconnect;
        Thread conecta = new Thread(new Runnable() {

            @Override
            public void run() {
                int intentos = 0;
                while (!nextConnected) {

                    try {
                        next = new Socket(nextNodeIp, nextPort);
                        nextConnected = true;
                        System.out.println("Conectado a " + nextNodeIp);
                        intentos = 0;
                        for (int i = 0; i < msg_queue.size(); i++) {
                            sendToNext(msg_queue.get(i));
                        }
                        sendToNext("is_closed;" + getMyIp());
                        msg_queue.clear();
                        receiveFromNext();
                        if (rec) {
                            sendToNext("reconnect");
                        }

                    } catch (UnknownHostException ex) {
                        System.out.printf("No es posible conectarse con el siguiente nodo, intento: %d\n", intentos++);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ex1) {
                        }
                    } catch (IOException ex) {
                        nextConnected = false;
                        System.out.printf("No es posible conectarse con el siguiente nodo, intento: %d\n", intentos++);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ex1) {
                        }
                    }
                }
            }
        });
        conecta.start();

    }

    public void listen() {
        Thread listen = new Thread(new Runnable() {

            @Override
            public void run() {

                try {
                    before = listenSocket.accept();
                    beforeConnected = true;
                    System.out.println("Conexion aceptada para " + before.getInetAddress().getHostAddress());
                    receiveFromBefore();

                } catch (IOException ex) {
                    beforeConnected = false;
                    System.out.println("Se desconecto el nodo anterior");
                }

            }
        });

        listen.start();
    }
}
