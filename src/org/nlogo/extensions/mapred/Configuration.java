package org.nlogo.extensions.mapred;

public class Configuration
{
	private String indir;
	private String outdir;
	
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
}

