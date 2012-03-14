package org.nlogo.extensions.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.nlogo.api.LogoList;

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
	
	private class ClientHandlerThread extends Thread
	{
		Socket client;
		
		public ClientHandlerThread(Socket client)
		{
			this.client= client;
		}
		
		public void run()
		{
			logger.debug("Client thread started " + client);
			try
			{
				BufferedReader in = new BufferedReader(
					new InputStreamReader( client.getInputStream()));
		        String inputLine;
		 
		        while ((inputLine = in.readLine()) != null)
		        {
		        	logger.debug("recieved: " + inputLine);
		        	Object key, val;
		        	String[] input;
		        	
		        	//parse the input
		        	input= inputLine.split(" ");
		        	key= val= null;
		        	switch(input[0].charAt(0))
		        	{
			        	case 'd':
		        			key= Double.parseDouble(input[1]);
		        			break;
			        	case 'i':
		        			key= Integer.parseInt(input[1]);
		        			break;
			        	case 's':
		        			key= input[1];
		        			break;
		        	}
		        	switch(input[2].charAt(0))
		        	{
			        	case 'd':
		        			val= Double.parseDouble(input[3]);
		        			break;
			        	case 'i':
		        			val= Integer.parseInt(input[3]);
		        			break;
			        	case 's':
		        			val= input[3];
		        			break;
		        	}
		        	
		        	if( MapRedProto.stage == MapRedProto.MAP_STAGE )
		    		{
		    			if( MapRedProto.map.containsKey(key))
		    			{
		    				MapRedProto.map.get(key).add(val);
		    			}
		    			else
		    			{
		    				ArrayList<Object> l= new ArrayList<Object>();
		    				l.add(val);
		    				MapRedProto.map.put(key, l);
		    			}
		    		}
		    		else if(MapRedProto.stage == MapRedProto.REDUCE_STAGE )
		    		{
		    			if( MapRedProto.rmap.containsKey(key))
		    			{
		    				//TODO: raise exception
		    			}
		    			else
		    			{
		    				MapRedProto.rmap.put(key, val);
		    			}
		    		}
		        }
			}
			catch(IOException e)
			{
				logger.error(e);
			}
			logger.debug("Client thread ended");
		}
	}
	
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
					new ClientHandlerThread(clientSocket).start();
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
		logger.debug(this.clientsock.toString());
		this.state= CLIENT;
	}
	
	public void runServer()
	{
		logger.debug("runServer");
		this.serverThread= new ServerThread(this.serversock);
		this.serverThread.start();
	}
	
	public void emit(Object key, Object val) throws IOException
	{
		logger.debug("emit <" + key.toString() + "," + val.toString() + ">");
		PrintWriter out= new PrintWriter(this.clientsock.getOutputStream(), true);
		
		out.println( buildDataString(out, key) + " " + buildDataString(out, val));
		out.flush();
	}
	
	private String buildDataString(PrintWriter out, Object data)
	{
		// logger.debug(data.getClass());
		if( data.getClass() == Double.class )
		{
			logger.debug("send double " + (Double) data);
			return "d " + data.toString();
		}
		else if( data.getClass() == Integer.class )
		{
			logger.debug("send integer " + (Integer) data);
			return "i " + data.toString();
		}
		else if( data.getClass() == String.class )
		{
			logger.debug("send string " + (String) data);
			return "s " + data.toString();
		}
		else if( data.getClass() == LogoList.class ); //TODO
		
		return "";
	}
}
