
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


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
    HashMap<String,String> msg_map;
    boolean serverFunction;
    boolean closeRing;
    
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
        serverFunction = false;
        this.listenPort=listenPort;
        this.nextPort = nextPort;
        this.nextNodeIp = nextIp;
        this.msg_map = new HashMap<String,String>();
        tryConnect();
        start();
    }
    
    public void sendToNext(String msg){
                  
        DataOutputStream out;
        try {
            out = new DataOutputStream(next.getOutputStream());
            out.writeUTF(msg);
        } catch (IOException ex) {
            
        }

    }
    public void sendToBefore(String msg){
                  
        DataOutputStream out;
        try {
            out = new DataOutputStream(before.getOutputStream());
            out.writeUTF(msg);
        } catch (IOException ex) {
            
        }

    }
    
    
    public void serverFunction(){
        Iterator it = msg_map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            System.out.println(pair.getValue());
            it.remove();
        }
    }

    public void receiveFromBefore(){
        
        Thread receive = new Thread(new Runnable() {

            @Override
            public void run() {
                String myIP = getMyIp();
                while(true){
                    
                    try {
                        DataInputStream in = new DataInputStream(before.getInputStream());
                        String msg = in.readUTF();
                        String data[] = msg.split(";");
                        
                        if(data[0].equals("is_closed") && !serverFunction){
                            if(data[1].equals(myIP)){
                                //soy servidor
                                serverFunction = true;
                                System.out.println("Se establece como servidor");
                                sendToBefore("set_server;"+getMyIp());
                            }else{
                                sendToNext(msg);
                            }
                        }
                        
                        
                        
                        
                        if(data[0].equals("msg")){
                            if(serverFunction){
                                msg_map.put(data[1], msg);
                            }
                            if(data[1].equals(myIP)){
                                // el mensaje ya dio la vuelta es decir se completo el envio en anillo y debe guardar el archivo
                                serverFunction();
                            }else{
                                //intenta enviar el mensaje al siguiente 

                                if (next!=null){
                                    sendToNext(msg);
                                }else{
                                    //el siguiente no esta entoncs nos volvemos servidor y le avisamos al resto que somos servidor,
                                    // a la vez que esperamos que nos digan quien es el ultimo para rehacer el anillo
                                    serverFunction = true;
                                    sendToBefore("get_last");
                                    sendToBefore("set_server;"+getMyIp());
                                }
                            }
                            
                            
                        }else if(data[0].equals("set_last")){
                            if(next!=null){
                                sendToNext(msg);
                            }else{
                                // recibio la informacion de quien es el ultimo entoncs reconecta el anillo
                                nextNodeIp = data[1];
                                nextPort = Integer.parseInt(data[2]);
                                tryConnect();
                            }
                        }
                        
                    } catch (IOException ex) {
                        System.out.println("Se desconecto el nodo anterior");
                        before = null;
                        beforeConnected = false;
                        start();
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
                        String data[] = msg.split(";");
                        if(data[0].equals("get_last")){
                            if(before!=null){
                                sendToBefore(msg);
                            }else{
                                sendToNext("set_last;"+getMyIp()+";"+listenPort);
                                
                            }
                        }else if(data[0].equals("set_server") && !serverFunction){
                            System.out.println("El servidor es: "+data[1]);
                            sendToBefore(msg);
                        }
                    } catch (IOException ex) {
                        System.out.println("Se desconecto el nodo siguiente");
                        next=null;
                        nextConnected=false;
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
                        sendToNext("is_closed;"+getMyIp());
//                        sendToNext("msg;"+getMyInfo());
                        
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
