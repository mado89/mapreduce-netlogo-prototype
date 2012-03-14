package org.nlogo.extensions.mapred;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.Syntax;
import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.LogoException;

public class NEmit extends DefaultCommand
{
	Logger logger = Logger.getLogger(NEmit.class);
	
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
			logger.error(e);
			e.printStackTrace();
		}
		
		try {
			MapRedProto.network.emit(key, val);
		} catch (IOException e) {
			throw new ExtensionException(e);
		}
	}
}

