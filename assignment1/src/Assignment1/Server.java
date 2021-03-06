/*
 * (c) University of Zurich 2014
 */

package Assignment1;
import java.net.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class Server {
	private static int port; 
		
	public static LinkedList<String> messageHistory = new LinkedList<String>();
	private static ServerSocket serverSocket;
	public static boolean listening = true;
	
	// Listen for incoming client connections and handle them
	public static void main(String[] args) {
		//port number to listen to
		port = Integer.parseInt(args[0]);
		

		// the server listens to incoming connections    
		// this is a blocking operation
		// which means the server listens to connections infinitely
		// when a new request is accepted, spawn a new thread to handle the request
		// keep listening to further requests
		// if you want, you can use the class HandleClient to process client requests
		// the first message for each new client connection is either "PRODUCER" or "LISTENER"
		try{
			serverSocket = new ServerSocket(port);
			serverSocket.setSoTimeout(0);
			while(listening){
				(new Thread(new HandleClient((serverSocket.accept())))).start();
			}
			serverSocket.close();
		}catch(SocketTimeoutException e){

		}catch(IOException e){
			
		}
	}
} 

// you can use this class to handle incoming client requests
// you are also free to implement your own class
class HandleClient extends Thread implements Runnable {
	private Socket server;
	
	HandleClient(Socket server){
		super();
		this.server = server;
	}
	public synchronized void run () {
		try{
			DataInputStream in = new DataInputStream(this.server.getInputStream());
			if(in.readUTF().equals("LISTENER")){
				//LISTENER
				int numberOfMessages = 0;

				DataOutputStream out = new DataOutputStream(server.getOutputStream());
				while(true){
					//SEND ALL UNREAD MESSAGES AND KEEP UPDATING
					while(numberOfMessages != Server.messageHistory.size()){
						String toSend = Server.messageHistory.get(numberOfMessages++);
						
						out.writeUTF(toSend);
					}
					wait(100);
				}
			}else{
				// PRODUCER
				while(true){
					String toReceive = in.readUTF();
					
					if(toReceive.contains(".bye")){
						break;
					}
					Server.messageHistory.add(toReceive);
				}				
			}	
		} catch(InterruptedException e){
			
		}catch(IOException e){

		}
    }
}
