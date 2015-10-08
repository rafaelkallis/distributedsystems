/*
 * (c) University of Zurich 2014
 */


package Assignment1;

import java.net.*;
import java.io.*;

public class Producer {
	public static void main(String[] args) {
		String serverName = args[0];
		int port = Integer.parseInt(args[1]);
		String clientName = args[2];
		String inputFileName = args[3];

		try {
			Socket client;
			DataOutputStream out;
			BufferedReader br;
			String line;
			
			// create connection to server
			client = new Socket(serverName,port);
			
			// send the string "PRODUCER" to server first
			out = new DataOutputStream(client.getOutputStream());
			out.writeUTF("PRODUCER");
			
			// read messages from input file line by line
			// put the client name and colon in front of each message
			// e.g., clientName:....
			// send message until you find ".bye" in the input file
			
			br = new BufferedReader(new FileReader(inputFileName));
			while ((line = br.readLine()) != null) {
				out.writeUTF(clientName+":"+line);
			    if(line.equals(".bye")){
			    	break;
			    }
			}
			
			// close connection
			br.close();
			client.close();
		} catch(UnknownHostException e){
			System.err.println("Host "+serverName+" unknown");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
