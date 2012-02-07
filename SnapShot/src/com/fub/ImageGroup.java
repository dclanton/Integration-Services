package com.fub;

import java.io.FileWriter;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;

public abstract class ImageGroup implements ConnectionManager
{
	private Properties props = new Properties();
	private String connectionURL;
	private Connection connection;
		
    abstract void build();
        
    public Connection getConnection()
    {
    	if( connection == null)
    	{
    		try 
            {
    			InputStream in = getClass().getResourceAsStream("resources.properties");  
    			props.load(in);		   
    			connectionURL = props.getProperty("connection.url");  			
    			DriverManager.registerDriver(new com.ibm.as400.access.AS400JDBCDriver());
    			connection = DriverManager.getConnection(connectionURL);					
            }
    		catch(Exception ex)
            {
    			System.out.println( "error " + ex.toString());
            }        		
    	}
    	return this.connection;
    }    
       
    public void write(String file, String msg) 
	{
    	try 
        {
            FileWriter aWriter = new FileWriter(file, true);
            aWriter.write( msg);
            aWriter.flush();
            aWriter.close();
        }
        catch (Exception e) 
        {
            System.out.println("write error " + e.toString() );
        }
    }
    
    public void log(String msg) 
	{
    	String log = "C:\\FUBGlobal360_xmlexport_Duplicates\\log.txt";    	
 		try 
        {
 			String DATE_FORMAT_NOW = "yyyy-MM-dd HH:mm:ss";
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
			FileWriter aWriter = new FileWriter(log, true);     
			aWriter.write( sdf.format(cal.getTime() ) + System.getProperty("line.separator"));
            aWriter.write( msg + System.getProperty("line.separator"));
            aWriter.flush();
            aWriter.close();
        }
        catch (Exception e) 
        {
            System.out.println("write error " + e.toString() );
        }
    }


}
