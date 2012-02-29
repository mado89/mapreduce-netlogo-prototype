package org.nlogo.extensions.mapred;

public class Configuration
{
	private String indir;
	private String outdir;
	private boolean writeOutput;
	
	public Configuration()
	{
		indir= outdir= "";
		writeOutput= false;
	}
	
	public String getInputDirectory()
	{
		return indir;
	}
	
	/**
	 * Sets the location of the directory of the input for the job
	 * @param indir The value to assign indir.
	 */
	public void setInputDirectory(String indir)
	{
		this.indir = indir;
	}
	
	/**
	 * Returns the value of outdir.
	 */
	public String getOutputDirectory()
	{
		return outdir;
	}
	
	/**
	 * Sets the location where the output of the job will be stored.
	 * @param outdir The value to assign outdir.
	 */
	public void setOutputDirectory(String outdir)
	{
		this.outdir = outdir;
	}
	
	/**
	 * Returns the if and output file should be written
	 */
	public boolean writeOutput()
	{
		return writeOutput;
	}


	/**
	 * Sets the if an outputfile should be written
	 * @param writeOutput The value to assign writeOutput.
	 */
	public void setWriteOutput(boolean writeOutput)
	{
		this.writeOutput = writeOutput;
	}
}

