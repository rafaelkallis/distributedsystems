/*
 * (c) University of Zurich 2014
 */


package Assignment1;

import java.net.*;
import java.io.*;

public class Listener
{
    public static void main(String [] args)
   {
      String serverName = args[0];
      int port = Integer.parseInt(args[1]);
      DataInputStream in ;
      DataOutputStream out;
      Socket client;
      
      try{
    	  // create connection to server
          client = new Socket(serverName,port);
          
    	  // send the string  "LISTENER" to server first!!
          out = new DataOutputStream(client.getOutputStream());
          out.writeUTF("LISTENER");
          
    	  // continuously receive messages from server
          // using stdout to print out messages Received
          // do not close the connection- keep listening to further messages from the server.
          in = new DataInputStream(client.getInputStream());
          while(true){
        	  System.out.println(in.readUTF());
          }
      }catch (UnknownHostException e){

      }catch (IOException e){

      }
   }
}
