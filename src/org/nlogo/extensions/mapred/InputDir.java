package org.nlogo.extensions.mapred;

import org.apache.log4j.Logger;

import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.Syntax;
import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.LogoException;

public class InputDir extends DefaultCommand
{
	Logger logger = Logger.getLogger(InputDir.class);
	
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {Syntax.StringType()});
	}
	
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		String val= "";
		
		try
		{
      val = args[0].getString();  
    }
    catch(LogoException e)
    {
      throw new ExtensionException( e.getMessage() ) ;
    }
    
    MapRedProto.config.setInputDirectory(val);
    logger.debug("Inputdir set to " + val);
	}
}

