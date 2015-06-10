
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;


/**
 *
 * @author David Morales
 */
public class RingNode extends Thread{
    String nextNodeIp;
    int nextPort;
    int listenPort;
    ServerSocket listenSocket;
    Socket next;
    Socket before;
    boolean beforeConnected;
    boolean nextConnected;
    ArrayList<String> msg_queue;
    
    public static void main(String[] args) {
        new RingNode(Integer.parseInt(args[0]),Integer.parseInt(args[1]) , args[2]);
    }
    
    public String getMyIp(){
        try {
            Enumeration<NetworkInterface> n = NetworkInterface.getNetworkInterfaces();
            while(n.hasMoreElements()){
                NetworkInterface e = n.nextElement();

                Enumeration<InetAddress> a = e.getInetAddresses();
                while (a.hasMoreElements())
                {
                    InetAddress addr = a.nextElement();
                    if(addr.getHostAddress().matches("\\d*\\.\\d*\\.\\d*\\.\\d*") && !addr.getHostAddress().equals("127.0.0.1")){
                        return addr.getHostAddress();
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return "";
    }
    
    public RingNode(int listenPort,int nextPort,String nextIp) {
       
        System.out.println(getMyIp());
        beforeConnected = false;
        nextConnected = false;
        this.listenPort=listenPort;
        this.nextPort = nextPort;
        this.nextNodeIp = nextIp;
        this.msg_queue = new ArrayList<String>();
        tryConnect();
        this.start();
    }
    
    public void sendToNext(String msg){
                  
        DataOutputStream out;
        try {
            out = new DataOutputStream(next.getOutputStream());
            out.writeUTF(msg);
        } catch (IOException ex) {
            
        }

    }

    public void receiveFromBefore(){
        String myIP = getMyIp();
        Thread receive = new Thread(new Runnable() {

            @Override
            public void run() {
                while(true){
                    
                    try {
                        DataInputStream in = new DataInputStream(before.getInputStream());
                        String msg = in.readUTF();
                        if (next!=null){
                            
                        }
                    } catch (IOException ex) {
                        System.out.println("Se desconecto el nodo anterior");
                    }
                
                }
            }
        });
        receive.start();
    }
    
    public void receiveFromNext(){
        Thread receive = new Thread(new Runnable() {

            @Override
            public void run() {
                while(true){
                    try {
                        DataInputStream in = new DataInputStream(next.getInputStream());
                        String msg = in.readUTF();
                    } catch (IOException ex) {
                        System.out.println("Se desconecto el nodo siguiente");
                        next=null;
                    }
                }
            }
        });
        receive.start();
    }
    
    public String getMyInfo(){
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        return getMyIp() + ";"+dateFormat.format(date); 
    }
    
    public void tryConnect(){
        Thread conecta = new Thread(new Runnable() {
            @Override
            public void run() {
                int intentos = 0;
                while(!nextConnected){
                    
                    try {
                        next = new Socket(nextNodeIp, nextPort);
                        nextConnected = true;
                        intentos = 0;
                        receiveFromNext();
                        
                        sendToNext(getMyInfo());
                        
                    } catch (UnknownHostException ex) {
                        System.out.printf("No es posible conectarse con el siguiente nodo, intento: %d\n",intentos++);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ex1) {
                        
                        }
                    } catch (IOException ex) {
                        nextConnected = false;
                        System.out.printf("No es posible conectarse con el siguiente nodo, intento: %d\n",intentos++);
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

    @Override
    public void run() {
        try {
            listenSocket = new ServerSocket(listenPort);
        } catch (IOException ex) {
            System.out.printf("El puerto %d esta siendo usado,", listenPort);
        }
        
        while(!beforeConnected){
            try {
                before = listenSocket.accept();
                beforeConnected = true;
                receiveFromBefore();
                
            } catch (IOException ex) {
                beforeConnected = false;
                System.out.println("Se desconecto el nodo anterior");
            }
        }
    }
    
}
