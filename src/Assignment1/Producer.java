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

		// create connection to server
		// send the string "PRODUCER" to server first
		// read messages from input file line by line
		// put the client name and colon in front of each message
		// e.g., clientName:....
		// send message until you find ".bye" in the input file
		// close connection
	}
}
