package com.fub;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;

public class CIF extends ImageGroup 
{
	private Connection connection;
	private ResultSet diimgSet;
	private Statement diimgStmt;
			
	@SuppressWarnings("unchecked")
	public void build() 
	{
		System.out.println( "Building CIF...");
		try
		{
      		CSVReader reader = new CSVReader(new FileReader("duplicates.txt"));   
    	  	List dupList = reader.readAll();
       		connection = getConnection();
      		diimgStmt =  connection.createStatement(); 			
			for( int x=0; x<dupList.size(); x++)
			{
				String[] val = (String[])dupList.get(x);
				String value = val[0];
			
				System.out.println( "processing " + value);
			    diimgSet =   diimgStmt.executeQuery("SELECT * from DATDUR.DIIMG WHERE (DIAIDX01#) = '4' AND DIAIDX01V = '" + value + "'"); 
			    while (diimgSet.next())
			    {
			    	StringBuffer buffer = new StringBuffer();
					buffer.append("OBJECTNUM " +    			diimgSet.getString("DIOBJNO") + "; ");
					buffer.append("OBJSOURCE " +    			diimgSet.getString("DIOBJSIP")+ "; ");
					buffer.append("FROMUSER " +     			diimgSet.getString("DIOBJFUSR") + "; ");
					buffer.append("EFFECTIVEDATE " +			diimgSet.getString("DIOBJEFF")+ "; ");
					buffer.append("OBJECTDATE " +   			diimgSet.getString("DIOBJDAT")+"; ");
					buffer.append("DIDOC# " +   			    diimgSet.getString("DIDOC#")+"; ");
					buffer.append("DIAIDX01V " +   			    diimgSet.getString("DIAIDX01V")+"; ");
					buffer.append("DIAIDX02V " +   			    diimgSet.getString("DIAIDX02V")+"; ");
					buffer.append("DIAIDX03V " +   			    diimgSet.getString("DIAIDX03V")+"; ");
					buffer.append("DIAIDX04V " +   			    diimgSet.getString("DIAIDX04V"));
					System.out.println( "writting " + buffer.toString());
					write("C:\\FUBGlobal360_xmlexport_Duplicates\\CIF\\duplicate.txt", buffer.toString() + '\n');					
			   }
		   }
			
		}
		catch(Exception ex)
		{
			System.out.println( "building error CIF " + ex.toString());
		}
		finally
        {
			cleanUp();	    
        }
		System.out.println( "Build Complete CIF ");		
	}
	
	public void cleanUp()
    {
    	try
    	{
    		if(diimgStmt != null)   diimgStmt.close();    
	   		if(diimgSet != null)    diimgSet.close();
    	    if(connection != null)  connection.close();
    	}
    	catch(Exception ex)
    	{
    		System.out.println("error closing connection CIF " + ex.toString());
    	}
    }
}
