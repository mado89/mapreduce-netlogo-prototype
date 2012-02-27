package org.nlogo.extensions.mapred;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import org.nlogo.api.CommandTask;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.Syntax;
import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.LogoException;
import org.nlogo.api.LogoListBuilder;

public class MapReduce extends DefaultCommand
{
	Logger logger = Logger.getLogger(InputDir.class);
	
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {
			Syntax.CommandTaskType() | Syntax.CommandBlockType(),
			Syntax.CommandTaskType() | Syntax.CommandBlockType()
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
			mapt= args[0].getCommandTask();
			redt= args[1].getCommandTask();
		}
		catch(LogoException e)
		{
			throw new ExtensionException(e.getMessage());
		}
		
		logger.debug("Inputfiles: " + list.toString());
		
		MapRedProto.stage= MapRedProto.MAP_STAGE;
		logger.debug("Mapping started");
		margs= new Object[1];
		for(i= 0; i < list.size(); i++)
		{
			margs[0]= list.get(i);
			mapt.perform(context, margs);
		}
		logger.debug("Mapping ended");
		logger.debug(MapRedProto.map.toString());
		
		//Go to reduce Stage
		MapRedProto.stage= MapRedProto.REDUCE_STAGE;
		
		//Reduce
		keys= MapRedProto.map.keySet().toArray();
		for(i= 0; i < keys.length; i++)
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
			redt.perform(context, l.toArray());
			logger.debug("Reducing " + keys[i] + " ended");
		}
		
		logger.debug("Reducing ended");
		logger.debug(MapRedProto.rmap.toString());
		
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

