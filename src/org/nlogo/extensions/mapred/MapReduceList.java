package org.nlogo.extensions.mapred;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.AbstractList;
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
import org.nlogo.api.LogoList;
import org.nlogo.api.LogoListBuilder;

public class MapReduceList extends DefaultCommand
{
	private class Job implements Runnable, Callable<Object>
	{
		CommandTask task;
		Object[] args;
		Context context;
		Logger logger = Logger.getLogger(MapReduceList.class);
		
		public Job(CommandTask task, Object[] args, Context context)
		{
			this.task= task;
			this.args= args;
			this.context= context;
		}
		
		public void run()
		{
			logger.debug("Starting " + task.toString() + " " + args[0].toString() );
			task.perform(context, args);
			logger.debug("Ended " + task.toString() + " " + args[0].toString() );
		}

		@Override
		public Object call() throws Exception
		{
			logger.debug("Starting " + task.toString() + " " + args[0].toString() );
			task.perform(context, args);
			logger.debug("Ended " + task.toString() + " " + args[0].toString() );
			return null;
		}
	}
	
	
	Logger logger = Logger.getLogger(MapReduceList.class);
	
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {
			Syntax.CommandTaskType() | Syntax.CommandBlockType(),
			Syntax.CommandTaskType() | Syntax.CommandBlockType(),
			Syntax.ListType()
		});
	}
	
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		CommandTask mapt;
		CommandTask redt;
		ArrayList<String> list = new ArrayList<String>();
		Object[] keys;
		Object[] margs;
		Object[] vals;
		int i, j;
		LogoList pvals; //passed values
		LogoList[] vall; //value lists
		String path;
		
		pvals= null;
		
		try
		{
			mapt= args[0].getCommandTask();
			redt= args[1].getCommandTask();
			pvals= args[2].getList();
		}
		catch(LogoException e)
		{
			throw new ExtensionException(e.getMessage());
		}
		
		if( pvals != null )
		{
			// Runtime.getRuntime().availableProcessors();
			
			ExecutorService pool= Executors.newFixedThreadPool(2);
			CompletionService<Object> complet= new ExecutorCompletionService<Object>(pool);
			
			/*int size= pvals.size();
			int half= size / 2;
			vall= new LogoList[2];
			vall[0]= pvals.logoSublist(0, half);
			logger.debug( vall[0].toString() );
			logger.debug( pvals.toString() );
			vall[1]= pvals.logoSublist(half, pvals.size());
			logger.debug( vall[1].toString() );*/
			vall= new LogoList[2];
			LogoListBuilder build = new LogoListBuilder();
			int half= pvals.size() / 2;
			for(i= 0; i < half; i++)
				build.add( pvals.get(i) );
			vall[0]= build.toLogoList();
			build = new LogoListBuilder();
			for(; i < pvals.size(); i++)
				build.add( pvals.get(i) );
			vall[1]= build.toLogoList();
			logger.debug( vall[0].toString() );
			logger.debug( vall[1].toString() );
			
			MapRedProto.resetMap();
			MapRedProto.stage= MapRedProto.MAP_STAGE;
			logger.debug("Mapping.list started");
			logger.debug(mapt.toString());
			for(i= 0; i < 2; i++)
			{
				margs= new Object[1];
				margs[0]= vall[i];
				// mapt.perform(context, margs);
				// complet.add( pool.submit(new Job(mapt, margs, context)) );
				complet.submit(new Job(mapt, margs, context));
				logger.debug("MapTask " + i + " submitted list size:" + vall[i].size());
			}
			logger.debug("All Map-Tasks submitted, waiting for completition");
			// pool.shutdown();
			try
			{
				pool.shutdown();
				complet.take();
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
			keys= MapRedProto.map.keySet().toArray();
			logger.debug("Keys " + keys.toString() );
			/*for(i= 0; i < keys.length; i++)
			{
				ArrayList<Object> l= new ArrayList<Object>();
				l.add(keys[i]);
				
				 // create a NetLogo list for the result
				 LogoListBuilder vlist = new LogoListBuilder();
				
				vals= MapRedProto.map.get(keys[i]).toArray();
				for(j= 0; j < vals.length; j++)
					vlist.add(vals[j]);
				
				l.add( vlist.toLogoList() );
				
				logger.debug("Reducing started for " + keys[i] + "(" + MapRedProto.map.get(keys[i]) + ")");
				logger.debug(redt.toString());
				redt.perform(context, l.toArray());
				logger.debug("Reducing " + keys[i] + " ended");
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
			}*/
		}
		else
		{
			logger.error("Something went wrong parsing arguments");
		}
	}
}

