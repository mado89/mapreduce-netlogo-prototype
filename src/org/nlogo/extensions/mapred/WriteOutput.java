package org.nlogo.extensions.mapred;

import org.apache.log4j.Logger;

import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.Syntax;
import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.LogoException;

public class WriteOutput extends DefaultCommand
{
	Logger logger = Logger.getLogger(WriteOutput.class);
	
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {Syntax.BooleanType()});
	}
	
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		boolean val= false;
		
		try
		{
      val = args[0].getBoolean();  
    }
    catch(LogoException e)
    {
      throw new ExtensionException( e.getMessage() ) ;
    }
    
    MapRedProto.config.setWriteOutput(val);
    logger.debug("WriteOutput set to " + val);
	}
}

