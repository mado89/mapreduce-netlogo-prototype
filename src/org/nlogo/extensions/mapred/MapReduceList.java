package org.nlogo.extensions.mapred;

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
import org.nlogo.api.Argument;
import org.nlogo.api.CommandTask;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoException;
import org.nlogo.api.LogoList;
import org.nlogo.api.LogoListBuilder;
import org.nlogo.api.Syntax;
import org.nlogo.headless.HeadlessWorkspace;

public class MapReduceList extends DefaultCommand
{
	private class MyJob implements Callable<Object>
	{
		String task;
		Object key;
		Object[] values;
		Logger logger = Logger.getLogger(MapReduceList.class);
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
			logger.debug("Starting " + task.toString() + " " + key.toString() + " " + values.toString() );
			
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
			s+= " " + key.toString(); //key
			s+= " [";
			for(int i= 0; i < values.length; i++ ) //value
				s+= " " + values[i].toString();
			s+= "]";
			logger.debug(s);
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
	
	Logger logger = Logger.getLogger(MapReduceList.class);
	private WSem wsem;
	
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {
			Syntax.StringType(),
			Syntax.StringType(),
			Syntax.ListType()
		});
	}
	
	public synchronized void perform(Argument args[], Context context) throws ExtensionException
	{
		String mapt;
		String redt;
		Object[] keys;
		Object[] vals;
		int i, j;
		LogoList lol, pvals; //passed values
		int jobC= 0;
		
		pvals= null;
		
		try
		{
			mapt= args[0].getString();
			redt= args[1].getString();
			lol= args[2].getList();
		}
		catch(LogoException e)
		{
			throw new ExtensionException(e.getMessage());
		}
		
		if( lol != null )
		{
			Object[] lola= lol.toArray();
			
			ExecutorService pool= Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
			CompletionService<Object> complet= new ExecutorCompletionService<Object>(pool);
			
			wsem= new WSem();
			wsem.aa();
			String world= wsem.getWorld();
			wsem= null;
			logger.debug("Exported");
			logger.debug("World: " + world.substring(0, 30));
			
			MapRedProto.resetMap();
			
			MapRedProto.stage= MapRedProto.MAP_STAGE;
			
			for(int l= 0; l < lola.length; l++)
			{
				LogoList ll= (LogoList) lola[l];
				logger.debug(ll.toString());
				Object key= ll.first();
				ll= ll.butFirst();
				pvals= (LogoList) ll.first();
				
				vals= new Object[2];
				int borders[];
				borders= new int[2];
				borders[0]= pvals.size() / 2;
				borders[1]= pvals.size();
				
				/*ArrayList<Object> build= new ArrayList<Object>();
				for(i= 0; i < half; i++)
				{
					build.add( pvals.first() );
					pvals= pvals.butFirst();
				}
				vals[0]= build.toArray();
				build = new ArrayList<Object>();
				for(; i < pvals.size(); i++)
				{
					build.add( pvals.first() );
					pvals= pvals.butFirst();
				}
				vals[1]= build.toArray();
				logger.debug( ((Object[]) vals[0]).toString() );
				logger.debug( vals[1].toString() );*/
			
				logger.debug("Mapping.list started");
				j= 0;
				for(i= 0; i < 2; i++)
				{
					ArrayList<Object> build= new ArrayList<Object>();
					for(; j < borders[i]; j++)
					{
						build.add( pvals.first() );
						pvals= pvals.butFirst();
					}
					// mapt.perform(context, margs);
					// complet.add( pool.submit(new Job(mapt, margs, context)) );
					complet.submit(new MyJob(mapt, key, build.toArray(), world));
					jobC++;
					logger.debug("MapTask " + i + " submitted list size:" + build.size());
				}
			}
			logger.debug("All Map-Tasks submitted, waiting for completition");

			try
			{
				pool.shutdown();
				for(int l= 0; l < jobC; l++)
					complet.take();
			}catch(InterruptedException e)
			{
				throw new ExtensionException( e );
			}
			logger.debug("Mapping.list ended");
			logger.debug(MapRedProto.map.toString());
			
			//Go to reduce Stage
			MapRedProto.stage= MapRedProto.REDUCE_STAGE;
			
			//Reduce
			pool= Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
			complet= new ExecutorCompletionService<Object>(pool);
			
			keys= MapRedProto.map.keySet().toArray();
			logger.debug("Keys " + keys.toString() );
			for(i= 0; i < keys.length; i++)
			{
				vals= MapRedProto.map.get(keys[i]).toArray();
				
				// mapt.perform(context, margs);
				// complet.add( pool.submit(new Job(mapt, margs, context)) );
				complet.submit(new MyJob(redt, keys[i], vals, world));
				logger.debug("ReduceTask " + i + " submitted list size:" + vals.length);
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
			
			/*if( MapRedProto.config.writeOutput() )
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
			}*/
		}
		else
		{
			logger.error("Something went wrong parsing arguments");
		}
	}
}

