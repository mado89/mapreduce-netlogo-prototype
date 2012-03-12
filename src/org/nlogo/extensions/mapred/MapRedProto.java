package org.nlogo.extensions.mapred;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

import org.nlogo.api.AgentException;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.ExtensionManager;
import org.nlogo.api.LogoException;

public class MapRedProto extends org.nlogo.api.DefaultClassManager
{
	public static Map<Object, ArrayList<Object>> map;
	public static Map<Object, Object> rmap;
	public static int stage;
	public static Configuration config;
	public static ExtensionManager em;
	public static org.nlogo.workspace.ExtensionManager em2;
	public static Network network;
	
	public final static int VOID_STAGE= 0;
	public final static int MAP_STAGE= 1;
	public final static int REDUCE_STAGE= 2;
	public final static int DONE_STAGE= 3;
	
	private static Logger logger;
	
  /**
  * Registers extension primitives.
  */
  public void load(org.nlogo.api.PrimitiveManager manager)
  {
     manager.addPrimitive("emit", new Emit());
     manager.addPrimitive("nemit", new NEmit());
     manager.addPrimitive("inputdir", new InputDir());
     manager.addPrimitive("outputdir", new OutputDir());
     manager.addPrimitive("writeoutput", new WriteOutput());
     manager.addPrimitive("map.linewise", new Emit());
     manager.addPrimitive("mapreduce", new MapReduce());
     manager.addPrimitive("mapreduce.list", new MapReduceList());
     
  }
  
  /**
	* Initializes this extension.
	*/
  public void runOnce(org.nlogo.api.ExtensionManager em) throws org.nlogo.api.ExtensionException
  {
    map= new HashMap<Object, ArrayList<Object>>();
    rmap= new HashMap<Object, Object>();
    
    logger = Logger.getLogger(MapRedProto.class);
    
    config= new Configuration();
    logger.debug("run Once");
    
    setupNetwork();
    
    // Set up a simple configuration that logs on the console.
    BasicConfigurator.configure();
    
    this.em= em;
    this.em2= (org.nlogo.workspace.ExtensionManager) em;
  }
  
  private void setupNetwork()
	{
		MapRedProto.network = new Network();
		try
		{
			network.initServer();
		}
		/*
		* TODO: Okay this is a really nasty work around What I do: If its not
		* possible to bind on the port, I assume that the server is allready
		* running, so i make that Workspace to a client
		*/
		catch (BindException e)
		{
			logger.debug("Port in use --> make it client");
			try
			{
				network.initClient("localhost"); // TODO: use real network adress
			} catch (IOException e1)
			{
				logger.error(e);
			} 
		} catch (Exception e)
		{
			logger.error(e);
		}
	}
  
  public static void resetMap()
  {
  	map= new HashMap<Object, ArrayList<Object>>();
    rmap= new HashMap<Object, Object>();
    logger.debug("maps reseted");
  }
  
  /**
  * To get a local file
  * copied from GISExtension
  */
  /*public static File getFile (String path)
  {
  	try
  	{
  		String fullPath = em.workspace().fileManager().attachPrefix(path);
  		if (em.workspace().fileManager().fileExists(fullPath))
  		{
  			return em.workspace().fileManager().getFile(fullPath);
  		}
        } catch (IOException e) { }
        return null;
  }*/
  
  /**
  *	Cleanup
  */
  public void unload(ExtensionManager em)
  {
    
  }
}

