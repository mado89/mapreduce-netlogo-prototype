package org.nlogo.extensions.mapred;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.Syntax;
import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.LogoException;

public class Emit extends DefaultCommand
{
	Logger logger = Logger.getLogger(Emit.class);
	
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {Syntax.WildcardType(), Syntax.WildcardType()});
	}
	
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		Object key= null;
		Object val= null;
		
		try
		{
			key= args[0].get();
			val= args[1].get();
		} catch (LogoException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		logger.debug(MapRedProto.stage + " <" + key + "," + val + ">");
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

