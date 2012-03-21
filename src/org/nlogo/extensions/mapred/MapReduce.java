package org.nlogo.extensions.mapred;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import org.nlogo.api.CommandTask;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.Syntax;
import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.LogoException;
import org.nlogo.api.LogoListBuilder;
import org.nlogo.headless.HeadlessWorkspace;

public class MapReduce extends DefaultCommand
{
	private class MyJob implements Callable<Object>
	{
		String task;
		Object key;
		Object[] values;
		Logger logger = Logger.getLogger(MyJob.class);
		String world;
		
		public MyJob(String task, Object key, Object[] values, String world)
		{
			this.task= task;
			this.key= key;
			this.values= values;
			this.world= new String(world); //just to be sure (for thread-safe)
		}
		
		@Override
		public Object call()
		{
			logger.debug("Starting " + task.toString() + " " + key.toString() );
			
			String model= MapRedProto.em2.workspace().getModelPath();
			logger.debug(model);
			
			HeadlessWorkspace ws = HeadlessWorkspace.newInstance();
			logger.debug("WS created");
			
			ws.open(model);
			logger.debug("Model opened");
			
			StringReader sr = new StringReader(world);
			logger.debug("Reader created");
			try {
				ws.importWorld(sr);
			} catch (IOException e) {
				logger.debug(e);
			}
			logger.debug("WS Imported");
			
			String s= task;
			if( key.getClass() == String.class )
				s+= " \"" + key.toString() + "\" ["; //key
			else
				s+= " " + key.toString() + " ["; //key
			for(int i= 0; i < values.length; i++ ) //value
			{
				if( values[i].getClass() == String.class )
					s+= " \"" + values[i].toString() + "\"";
				else
					s+= " " + values[i].toString();
			}
			s+= "]";
			// logger.debug(s);
			ws.command(s);
			
			try
			{
				ws.dispose();
			} catch (InterruptedException e)
			{
				logger.debug(e);
			}
			
			logger.debug("Ended " + task.toString() + " " + key.toString() );
			return null;
		}
	}
	
	// TODO: explain Semaphor
	private class WSem
	{
		private String world= "";
		private final Object sync= new Object();
		private boolean exportRunning= false;
		
		public void aa()
		{
			org.nlogo.awt.EventQueue.invokeLater(new Runnable()
			{
				public void run()
				{
					try
					{
						synchronized(sync)
						{
							StringWriter sw = new StringWriter();
							MapRedProto.em2.workspace().exportWorld(new PrintWriter(sw));
							logger.debug("exported");
							world = sw.toString();
							exportRunning= false;
							sync.notifyAll();
							logger.debug("notified");
						}
					}
					catch(IOException io)
					{
						logger.error(io);
					}
				}
			});
		}
		
		public String getWorld()
		{
			logger.debug("exR " + exportRunning);
			synchronized( sync )
			{
				while( exportRunning || world.equals("") )
				{
					try {
						logger.debug("wait");
						sync.wait();
						logger.debug("waited");
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			return world;
		}
	}
	
	Logger logger = Logger.getLogger(MapReduce.class);
	private WSem wsem;
	
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {
			Syntax.StringType(),
			Syntax.StringType()
		});
	}
	
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		String mapt;
		String redt;
		ArrayList<String> list = new ArrayList<String>();
		Object[] keys,vals;
		int i;
		int jobC;
		
		String path= MapRedProto.em.getFile(MapRedProto.config.getInputDirectory()).getPath();
		File dir = new File(path);
		
		logger.debug(dir);
		logger.debug(path);
		
		String[] children = dir.list();
		if (children != null)
		{
			for (i=0; i < children.length; i++)
			{
				//TODO: sperator os dependend
				list.add(path + "/" + children[i]);
			}
		}
		else
		{
			//TODO: empty directory
			logger.warn("empty input directory ('" + MapRedProto.config.getInputDirectory() + "')");
		}
		
		try
		{
			mapt= args[0].getString();
			redt= args[1].getString();
		}
		catch(LogoException e)
		{
			throw new ExtensionException(e.getMessage());
		}
		
		logger.debug("Inputfiles: " + list.toString());
		
		ExecutorService pool= Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		CompletionService<Object> complet= new ExecutorCompletionService<Object>(pool);
		jobC= 0;
		
		wsem= new WSem();
		wsem.aa();
		String world= wsem.getWorld();
		wsem= null;
		logger.debug("Exported");
		
		MapRedProto.resetMap();
		MapRedProto.stage= MapRedProto.MAP_STAGE;
		logger.debug("Mapping started");
		for(i= 0; i < list.size(); i++)
		{
			String fn= list.get(i);
			ArrayList<String> h= new ArrayList<String>();
			//String s= "\"";
			
			try
			{
				File file= new File(fn);
				BufferedReader in= new BufferedReader(new FileReader(file));
				// while( in.)
				String line;
				while((line= in.readLine()) != null)
				{
					//s+= line;
					// mapt.perform(context, margs);
					if( line.length() > 0 )
						h.add(line.replace("\"", "\\\""));
				}
				//s+= "\"";
			} catch (FileNotFoundException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			complet.submit(new MyJob(mapt, fn, (Object[]) h.toArray(), world));
			jobC++;
		}
		
		try
		{
			pool.shutdown();
			for(int l= 0; l < jobC; l++)
				complet.take();
		}catch(InterruptedException e)
		{
			throw new ExtensionException( e );
		}
		logger.debug("Mapping ended");
		logger.debug(MapRedProto.map.toString());
		
		//Go to reduce Stage
		MapRedProto.stage= MapRedProto.REDUCE_STAGE;
		pool= Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		complet= new ExecutorCompletionService<Object>(pool);
		
		//Reduce
		keys= MapRedProto.map.keySet().toArray();
		for(i= 0; i < keys.length; i++)
		{
			/*ArrayList<Object> l= new ArrayList<Object>();
			l.add(keys[i]);
			
			 // create a NetLogo list for the result
			 LogoListBuilder vlist = new LogoListBuilder();
			
			vals= MapRedProto.map.get(keys[i]).toArray();
			for(j= 0; j < vals.length; j++)
				vlist.add(vals[j]);
			
			l.add( vlist.toLogoList() );*/
			vals= MapRedProto.map.get(keys[i]).toArray();
			
			logger.debug("Reducing started for " + keys[i]);
			// redt.perform(context, l.toArray());
			complet.submit(new MyJob(redt, keys[i], vals, world));
			logger.debug("Reducing " + keys[i] + " ended");
		}
		
		logger.debug("All Map-Tasks submitted, waiting for completition");
		// pool.shutdown();
		try
		{
			pool.shutdown();
			for(i= 0; i < keys.length; i++)
				complet.take();
		}catch(InterruptedException e)
		{
			throw new ExtensionException( e );
		}
		logger.debug("Reducing ended");
		logger.debug(MapRedProto.rmap.toString());
		
		if( MapRedProto.config.writeOutput() )
		{
			//Write Output
			try
			{
				path= MapRedProto.em.getFile(MapRedProto.config.getOutputDirectory()).getPath();
				path= MapRedProto.em.getFile("./").getPath();
				File file= new File(path + "/output.txt");
				BufferedWriter out= new BufferedWriter(new FileWriter(file));
				keys= MapRedProto.rmap.keySet().toArray();
				for(i= 0; i < keys.length; i++)
				{
					// vals= MapRedProto.map.get(keys[i]);
					out.write(keys[i] + ": " + MapRedProto.rmap.get(keys[i]) + "\n");
				}
				out.close();
			}
			catch(IOException e)
			{
				throw new ExtensionException( e );
			}
		}
	}
}

