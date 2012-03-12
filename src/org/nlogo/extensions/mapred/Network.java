package org.nlogo.extensions.mapred;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;

/**
 * A Class to communicat with other instances
 * @author martin
 */
public class Network
{
	private int state;
	private ServerSocket serversock;
	private Socket clientsock;
	private Logger logger;
	
	public static int PORT_NR = 4455;
	
	public static final int NONE =   0;
	public static final int SERVER = 1;
	public static final int CLIENT = 2;
	
	private Thread serverThread;
	
	private class ServerThread extends Thread
	{
		private ServerSocket serversock;
		
		public ServerThread(ServerSocket serverSocket)
		{
			this.serversock= serverSocket;
		}
		
		public void run()
		{
			Socket clientSocket = null;
			try
			{
				while(true)
				{
					clientSocket = this.serversock.accept();
					logger.debug("New Client " + clientSocket.toString());
				}
			} 
			catch (IOException e)
			{
			    System.out.println("Accept failed: 4444");
			    System.exit(-1);
			}
		}
	}
	
	public Network()
	{
		this.state= NONE;
		this.logger= Logger.getLogger(Network.class);
		this.serverThread= null;
	}
	
	public int getState()
	{
		return this.state;
	}
	
	public void initServer() throws IOException
	{
		logger.debug("initServer");
		this.serversock= new ServerSocket(PORT_NR);
		this.state= SERVER;
	}
	
	public void initClient(String addr) throws IOException
	{
		logger.debug("initclient");
		this.clientsock= new Socket(addr, PORT_NR);
		this.state= CLIENT;
	}
	
	public void runServer()
	{
		logger.debug("runServer");
		this.serverThread= new ServerThread(this.serversock);
		this.serverThread.run();
	}
	
	public void emit(Object key, Object val) throws IOException
	{
		logger.debug("emit <" + key.toString() + "," + val.toString() + ">");
		PrintWriter out= new PrintWriter(this.clientsock.getOutputStream(), true);
		
		sendData(out, key);
		sendData(out, val);
	}
	
	private void sendData(PrintWriter out, Object data)
	{
		logger.debug(data.getClass());
	}
}
