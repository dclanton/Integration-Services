package com.fub;

import java.io.FileWriter;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;

public class Snapshot 
{
	private Properties props = new Properties();
	private static String connectionURL;
	private static Connection connection;
	private static Statement stmt;
	private static Statement stmt2; 
	private static ResultSet diimgSet;
	private static ResultSet cfmastSet;
	private static String file = "C:\\Snapshot\\no_match_CIF.txt";
	
	public static void main(String[] args) 
	{
		Snapshot ss = new Snapshot();
		ss.init();
		ss.runReport();
	}
	
	public void init()
	{
		try 
        {
			InputStream in = getClass().getResourceAsStream("resources.properties");  
			props.load(in);		   
			connectionURL = props.getProperty("connection.url");  	   	
		
			DriverManager.registerDriver(new com.ibm.as400.access.AS400JDBCDriver());
			connection = DriverManager.getConnection(connectionURL);
			stmt = connection.createStatement(); 
			stmt2 = connection.createStatement();
			diimgSet = stmt.executeQuery("Select DIOBJNO, DIOBJFUSR, DIOBJDAT, (DIDOC#), DIAIDX01V, DIAIDX02V, DIAIDX03V, DIAIDX04V from DATDUR.DIIMG WHERE (DIAIDX01#) = '4' AND LENGTH(RTRIM(DIAIDX01V)) = '9' "); 
			System.out.println( "Set 1 done");			

        }
		catch(Exception ex)
        {
			System.out.println( "error " + ex.toString());
        }
	}
	
	public void runReport() 
	{
		System.out.println( "running report");
		int match = 0;
        int noMatch = 0;   
        int count = 0;
        boolean empty = true;
        String DATE_FORMAT_NOW = "yyyy-MM-dd HH:mm:ss";
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
		write( file, sdf.format(cal.getTime() ) );
      	try
		{			
			while (diimgSet.next() && count < 10000)
			{
				count++;
				empty = true;
				int val = new Double(diimgSet.getString("DIAIDX01V").trim()).intValue();
			    cfmastSet = stmt2.executeQuery("Select RTRIM(CFSSNO) AS SSN from DATDUR.CFMAST WHERE RTRIM(CFSSNO)='" + val + "'" ); 
				while (cfmastSet.next())
				{
					match++;
					empty = false;				
					break;					
				}
				if(empty)
				{
					noMatch++;
					/*
					write(file, count + ". "   +
							    " DIOBJNO "    +    diimgSet.getString("DIOBJNO").trim()    + 
						        ", DIOBJFUSR " +    diimgSet.getString("DIOBJFUSR").trim()  +
					            ", DIOBJDAT "  +    diimgSet.getString("DIOBJDAT").trim()   +
					            ", DIDOC# "    +    diimgSet.getString("DIDOC#").trim()     +
					            ", DIAIDX01V " +    diimgSet.getString("DIAIDX01V").trim()  +
					            ", DIAIDX02V " +    diimgSet.getString("DIAIDX02V").trim()  +
					            ", DIAIDX03V " +    diimgSet.getString("DIAIDX03V").trim()  +
					            ", DIAIDX04V " +    diimgSet.getString("DIAIDX04V").trim()  );
					            */
				}
					
			}				
			System.out.println( "matches " + match + " no matches " + noMatch);		
	       
		}
		catch(Exception ex)
		{
			System.out.println( "runReport error " + ex.toString());
		}
		finally
        {
        	try
        	{
        		if(stmt != null)        stmt.close();    
    			if(stmt2 != null)       stmt2.close();    
        		if(diimgSet != null)    diimgSet.close();
        		if(cfmastSet != null)   cfmastSet.close();
    			if(connection != null)  connection.close();
        	}
        	catch(SQLException sqx)
        	{
        		System.out.println("Error closing connection: " + sqx.toString());
        	}         	
        }
		System.out.println( "Report Complete");
	}
	
	/*
	@SuppressWarnings({ "finally", "unchecked" })
	public void runReport() 
	{
		Statement stmt2 = null;
		ResultSet rs = null;
		ResultSet rs2 = null;
	    ArrayList letters = new ArrayList();
	    ArrayList types = new ArrayList();
	    ArrayList names = new ArrayList();
	    ArrayList invalidList = new ArrayList();
	    ArrayList noMatch = new ArrayList();
	    ArrayList objNum = new ArrayList();
	    ArrayList docNum = new ArrayList();
		try 
        {
			InputStream in = getClass().getResourceAsStream("resources.properties");  
			props.load(in);		   
			connectionURL = props.getProperty("connection.url");  	   	
			
        	DriverManager.registerDriver(new com.ibm.as400.access.AS400JDBCDriver());
            connection = DriverManager.getConnection(connectionURL);
            stmt = connection.createStatement(); 
            stmt2 = connection.createStatement(); 
         
			rs = stmt.executeQuery("Select count(DIAIDX01V) AS ALLCOUNT from DATDUR.DIIMG" ); 
			while (rs.next())
			{
				write("Total Count DIIMG: " + rs.getString("ALLCOUNT"));
				System.out.println( "DONE Total Count DIIMG:");
			}	
			rs = stmt.executeQuery("Select count(DIAIDX01V) AS ALLCOUNT from DATDUR.DIIMG WHERE (DIAIDX01#) <> '4'" ); 
			while (rs.next())
			{
				write("Account Level Count (end with letter):  " + rs.getString("ALLCOUNT"));
				System.out.println( "DONE Account Level Count");
			}	
			rs = stmt.executeQuery("Select count(DIAIDX01V) AS ALLCOUNT from DATDUR.DIIMG WHERE (DIAIDX01#) <> '1'" ); 
			while (rs.next())
			{
				write("Customer Level Count (end with no letter):  " + rs.getString("ALLCOUNT"));
				System.out.println( "DONE Customer Level Count ");
			}	
			rs = stmt.executeQuery("Select distinct (SUBSTRING(DIAIDX01V, (LENGTH(RTRIM(DIAIDX01V))), 1)) AS LETTER from DATDUR.DIIMG WHERE (DIAIDX01#) <> '4'");
			while (rs.next())
			{
				letters.add(rs.getString("LETTER"));
			}	
			String message = "Account Suffix Letters: ";
			for( int x = 0; x< letters.size(); x++ )
			{
				if( x != ( letters.size() - 1))	message += letters.get(x) + ", ";
				else message += letters.get(x);				
			}
			write(message);
			System.out.println( "DONE Letters ");			
			for( int x = 0; x< letters.size(); x++ )
			{
				rs = stmt.executeQuery("Select count(DIAIDX01V) AS ALLCOUNT from DATDUR.DIIMG WHERE SUBSTRING(DIAIDX01V, (LENGTH(RTRIM(DIAIDX01V))), 1) = '" + letters.get(x) + "'" ); 
				while (rs.next())
				{
					write(letters.get(x) + " count: " + rs.getString("ALLCOUNT"));						
				}
			}
			System.out.println( "DONE Count Of Letter");
			write("*****************************************");
			write("******    ACCOUNT DOCUMENTS      ********");
			write("*****************************************");
			for( int x = 0; x< letters.size(); x++ )
			{
				rs = stmt.executeQuery("Select distinct(DIDOC#) AS TYPE from DATDUR.DIIMG WHERE SUBSTRING(DIAIDX01V, (LENGTH(RTRIM(DIAIDX01V))), 1) = '" + letters.get(x) + "'" ); 
				write("--------------");				
				write(letters.get(x) + " CONTAINS TYPES:");
				while (rs.next())
				{
					String val = rs.getString("TYPE");
					rs2 = stmt2.executeQuery("Select count(DIAIDX01V) AS COUNT from DATDUR.DIIMG WHERE SUBSTRING(DIAIDX01V, (LENGTH(RTRIM(DIAIDX01V))), 1) = '" + letters.get(x) + "'" +  "AND (DIDOC#) = '" + val + "'");
					while(rs2.next())
					{
						write( "DOCTYPE " + val + ": " + rs2.getString("COUNT") );
					}
				}				
			}
			System.out.println( "DONE Count Of Types By Letter");
			rs2.close();
			write("*****************************************");
			write("*******      CIF DOCUMENTS      *********");
			write("*****************************************");
			rs = stmt.executeQuery("Select distinct (DIDOC#) AS TYPE from DATDUR.DIIMG WHERE (DIAIDX01#) = '4'" ); 
			while (rs.next())
			{
				types.add(rs.getString("TYPE"));
			}	
			for( int x = 0; x< types.size(); x++ )
			{
				rs = stmt.executeQuery("Select count(DIAIDX01V) AS COUNT from DATDUR.DIIMG WHERE (DIDOC#) = '" + types.get(x) + "'"); 
				while (rs.next())
				{
					
					write("DOCTYPE " + types.get(x) + ": " + rs.getString("COUNT") );
					
				}	
			}	
			System.out.println( "DONE CIF Documents");
			write("Count of Document Level = 4 and No TaxID/Account Number");
			types.clear();
			rs = stmt.executeQuery("Select distinct (DIDOC#) AS TYPE from DATDUR.DIIMG WHERE LENGTH(RTRIM(DIAIDX01V)) = '0' AND (DIAIDX01#) = '4'");
			while (rs.next())
			{
				types.add(rs.getString("TYPE"));
			}
			for( int x = 0; x< types.size(); x++ )
			{
				rs = stmt.executeQuery("Select count(DIAIDX01V) AS COUNT from DATDUR.DIIMG WHERE (DIDOC#) = '" + types.get(x) + "' AND LENGTH(RTRIM(DIAIDX01V)) = '0' AND (DIAIDX01#) = '4'"); 
				while (rs.next())
				{
					write("DOCTYPE " + types.get(x) + ": " + rs.getString("COUNT") );					
				}	
			}	
			System.out.println( "DONE no TaxID/Account Number");
			write("Count of Document Level = 4 and TaxID/Account Number = 0000000000");
			types.clear();
			rs = stmt.executeQuery("Select distinct (DIDOC#) AS TYPE from DATDUR.DIIMG WHERE(DIAIDX01V)='0000000000' AND (DIAIDX01#)='4'");
			while (rs.next())
			{
				types.add(rs.getString("TYPE"));
			}
			for( int x = 0; x< types.size(); x++ )
			{
				rs = stmt.executeQuery("Select count(DIAIDX01V) AS COUNT from DATDUR.DIIMG WHERE (DIDOC#) = '" + types.get(x) + "' AND (DIAIDX01V)='0000000000' AND (DIAIDX01#) = '4'"); 
				while (rs.next())
				{
					write("DOCTYPE " + types.get(x) + ": " + rs.getString("COUNT") );					
				}	
			}	
			System.out.println( "DONE TaxID/Account Number 0000000000");
			rs = stmt.executeQuery("Select count(*) AS ALLCOUNT from DATDUR.DIIMG WHERE LENGTH(RTRIM(DIAIDX01V)) = '0' AND (DIAIDX01#) = '1'" ); 
			while (rs.next())
			{
				write("Count of Document Level = 1 and No TaxID/Account Number:  " + rs.getString("ALLCOUNT"));				
			}
			System.out.println( "DONE Document Level = 1 And No TaxID/Account Number");
			rs = stmt.executeQuery("Select count(*) AS ALLCOUNT from DATDUR.DIIMG WHERE(DIAIDX01V)='0000000000' AND (DIAIDX01#) = '1'" ); 
			while (rs.next())
			{
				write("Count of Document Level = 1 and TaxID/Account Number 0000000000:  " + rs.getString("ALLCOUNT"));				
			}
			System.out.println( "DONE Document Level = 1 And TaxID/Account Number 0000000000");
			rs = stmt.executeQuery("Select count(*) AS ALLCOUNT from DATDUR.DIIMG WHERE LENGTH(RTRIM(DIAIDX01V))>= '10' AND (DIAIDX01#)='4'" );
			while (rs.next())
			{
				write("Count of TaxID/Account Number >= 10 chars:  " + rs.getString("ALLCOUNT"));				
			}
			System.out.println( "DONE Count of TaxID/Account Number >= 10 Chars");
		
            System.out.println( "STARTING");
            String DATE_FORMAT_NOW = "yyyy-MM-dd HH:mm:ss";
            Calendar cal = Calendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
            //write("------------------------");	
            //write("------------------------");	
            //write("Report Run At: " + sdf.format(cal.getTime()));
            System.out.println("Report Run At: " + sdf.format(cal.getTime()));
            int matches = 0;
            int start = 1;
            int noMatches = 0;
            int invalidChars = 0;
            boolean empty = true;
      
            rs = stmt.executeQuery("Select DIOBJNO, DIOBJFUSR, (DIDOC#),(DIAIDX01V), (DIAIDX02V), (DIAIDX03V), (DIAIDX04V) from DATDUR.DIIMG WHERE (DIAIDX01#)<>'4' AND SUBSTRING(DIAIDX01V, (LENGTH(RTRIM(DIAIDX01V))), 1) = 'A' AND (DIDOC#)<>'1043'");
            while (rs.next())
			{
            	write(start + ". " + rs.getString("DIOBJNO") + " " + rs.getString("DIOBJFUSR") +  rs.getString("DIDOC#") +" " + rs.getString("DIAIDX01V") + " " + rs.getString("DIAIDX02V") + " " + rs.getString("DIAIDX03V") + " "+ rs.getString("DIAIDX04V"));
			   	matches++;
			   	start++;
			}
            write("Total Rows Found: " + matches);
        
            //rs = stmt.executeQuery("Select (DIDOC#) AS DOCNUM, RTRIM(DIOBJNO) AS OBJNUM, RTRIM(SUBSTRING(DIAIDX01V, 2, LENGTH(RTRIM(DIAIDX01V)))) AS ZEROTRIMMED, RTRIM(DIAIDX02V) AS NAME from DATDUR.DIIMG WHERE LENGTH(RTRIM(DIAIDX01V))>= '10' AND (DIAIDX01#)='4' AND (DIAIDX01V) <> '0000000000'");
          
            while (rs.next())
			{
				types.add(rs.getString("ZEROTRIMMED"));
				names.add(rs.getString("NAME"));
				objNum.add(rs.getString("OBJNUM"));
				docNum.add(rs.getString("DOCNUM"));
			}
            rs = stmt.executeQuery("Select SUBSTRING(DIAIDX01V, 0, 10) AS ZEROTRIMMED, RTRIM(DIAIDX02V) AS NAME from DATDUR.DIIMG WHERE LENGTH(RTRIM(DIAIDX01V))= '10' AND (DIAIDX01#)='4' AND (DIAIDX01V) <> '0000000000' AND  DIOBJEFF <= '20100217'");
            while (rs.next())
			{
				types.add(rs.getString("ZEROTRIMMED"));
				names.add(rs.getString("NAME"));
			}
            
		
		
			System.out.println( "MATCH BOTH");
			//write("MATCH FOR SSN AND NAME IN DIIMG AND CFMAST");
			//write("------------------------");	
			//write("------------------------");	
			for( int x = 0; x< types.size(); x++ )
			{
				String val = (String)names.get(x);
				if( val.indexOf('\'') > 0 )
				{
					invalidChars++;
					invalidList.add(names.get(x));
					continue;
				}
				empty = true;
				rs = stmt.executeQuery("Select RTRIM(CFSNME) AS NAME, RTRIM(CFSSNO) AS SSN from DATDUR.CFMAST WHERE RTRIM(CFSSNO)='" + types.get(x) + "'" );
				//rs = stmt.executeQuery("Select RTRIM(CFSNME) AS NAME, RTRIM(CFSSNO) AS SSN from DATDUR.CFMAST WHERE RTRIM(CFSSNO)='" + types.get(x) + "' AND RTRIM(CFSNME)='" + val + "'" );
				while (rs.next())
				{
					//write("DIIMG TAXID: " + types.get(x) + ", NAME: " + names.get(x) + " => CFMAST TAXID: " + rs.getString("SSN") + " NAME: " + rs.getString("NAME"));					
					matches++;
					empty = false;
				}	
				if(empty)
				{
					noMatches ++;
					//noMatch.add("DIIMG DIOBJNO " + objNum.get(x)  + " TAXID: " + types.get(x) + ", NAME: " + names.get(x));						
				}
			}
			System.out.println("Total Count of DIIMG DIAIDX01V with length > 9 and not '0000000000': " + types.size());
			System.out.println("Total matches on both: " + matches);
			System.out.println("No Match on both: " + noMatches);
			System.out.println("Total invalid chars - not processed: " + invalidChars);
		
			write("NO MATCH FOR SSN AND NAME IN DIIMG AND CFMAST");
			write("------------------------");	
			write("------------------------");	
			for( int x = 0; x< noMatch.size(); x++ )
			{
				write((String)noMatch.get(x));
			}
			write("------------------------");	
			write("------------------------");	
			write("Total Count of DIIMG DIAIDX01V with length > 9 and not '0000000000': " + types.size());
			write("Total matches on both: " + matches);
			write("No Match on both: " + noMatches);
			write("Total invalid chars - not processed: " + invalidChars);
			int total = invalidChars + matches + noMatches;
			write("Total: " + total );
			write("------------------------");	
			//write("No match found For TAXID & NAME:");
			//for( int x = 0; x< noMatch.size(); x++ )
			//{
			//	write((String)noMatch.get(x));
			//}
			write("------------------------");	
	      
			
		
			matches = 0;
			noMatches = 0;
			invalidChars = 0;	
			int total = 0;
            System.out.println( "MATCH SSN");
            write("MATCH FOR SSN");
			write("------------------------");	
			write("------------------------");	
			for( int x = 0; x< types.size(); x++ )
			{
				String val = (String)names.get(x);
				if( val.indexOf('\'') > 0 )
				{
					invalidChars++;
					invalidList.add(names.get(x));
					continue;
				}
				empty = true;
				rs = stmt.executeQuery("Select RTRIM(CFSNME) AS NAME, RTRIM(CFSSNO) AS SSN from DATDUR.CFMAST WHERE RTRIM(CFSSNO)='" + types.get(x) + "'" );
				while (rs.next())
				{
					write("DIIMG DOC NUMBER: " + docNum.get(x) + " DIIMG TAXID|SSN: " + types.get(x) + " DIIMG NAME: " + names.get(x) + " CFMAST SSN:" + rs.getString("SSN") + " CFMAST NAME:" + rs.getString("NAME"));					
					empty = false;
					matches++;					
				}	
				if(empty)
				{					
					noMatches++;	
					//System.out.println("OBJ NO " + objNum.get(x) );
					rs = stmt.executeQuery("Select RTRIM(DIOBJNO) AS OBJNO, DIOBJFUSR AS DUSER,(DIDOC#) AS DOCNUMBER,(DIAIDX01V) AS TAXID,(DIAIDX02V) AS NAME from DATDUR.DIIMG WHERE RTRIM(DIOBJNO)='" + objNum.get(x) + "'" );
		            while (rs.next())
					{
		            	//System.out.println("OBJ NO " + rs.getString("DIOBJNO"));
		            	//System.out.println("USER " + rs.getString("DIOBJFUSR"));
		            	//System.out.println("DOC NO " + rs.getString("DIDOC#"));
		            	//System.out.println("TAXID " + rs.getString("DIAIDX01V"));
		            	//System.out.println("NAME " + rs.getString("DIAIDX02V"));
		            	
		            	String value = "DIIMG DOC NUMBER: " + rs.getString("OBJNO") + " DIIMG USER: " + rs.getString("DUSER") + " DIIMG DOC NUMBER: "+ rs.getString("DOCNUMBER") + " DIIMG TAXID|SSN: " + rs.getString("TAXID") + " DIIMG NAME: " + rs.getString("NAME");
		            	noMatch.add(value);		           	   
					}		   
				}
			}
			write("Total matches on TAXID " + matches);
			write("No Match on TAXID: " + noMatches);
			write("Total invalid chars - not processed: " + invalidChars);
			total = invalidChars + matches + noMatches;
			write("Total: " + total );
			write("------------------------");	
			
			write("No match found for TAXID ");
			for( int x = 0; x< noMatch.size(); x++ )
			{
				write((String)noMatch.get(x));
			}
			write("------------------------");	
		
			write("No TAXID Match In CFMAST For:");	
			for( int x = 0; x< noMatch.size(); x++ )
			{
				empty = true;
	            rs = stmt.executeQuery("Select DIOBJNO, DIOBJFUSR, (DIDOC#),(DIAIDX01V), (DIAIDX02V), (DIAIDX03V), (DIAIDX04V) from DATDUR.DIIMG WHERE RTRIM(DIAIDX01V)='" + noMatch.get(x) );
	            while (rs.next())
				{
	            	write(start + ". " + rs.getString("DIOBJNO") + " " + rs.getString("DIOBJFUSR") +  rs.getString("DIDOC#") +" " + rs.getString("DIAIDX01V") + " " + rs.getString("DIAIDX02V") + " " + rs.getString("DIAIDX03V") + " "+ rs.getString("DIAIDX04V"));
				   	matches++;
				   	start++;
				}
			}
		
		
			matches = 0;
			noMatches = 0;
			invalidChars = 0;
			total = 0;
			//noMatch.clear();
			System.out.println( "MATCH NAMES");
			System.out.println(  names.size());
			write("DIIMG                 CFMAST");
			for( int x = 0; x< names.size(); x++ )
			{
				String val = (String)names.get(x);
				if( val.indexOf('\'') > 0 )
				{
					invalidChars++;
					continue;
				}
				empty = true;
				rs = stmt.executeQuery("Select RTRIM(CFSNME) AS NAME from DATDUR.CFMAST WHERE RTRIM(CFSNME)='" + val + "'" );
				while (rs.next())
				{
					write(val + " => " + rs.getString("NAME"));					
					empty = false;
					matches++;
				}	
				if(empty)
				{
					//noMatch.add(types.get(x) + " : " + names.get(x));	
					noMatches++;					
				}
			}
			write("Total matches between NAME: " + matches);
			write("No Match on NAME: " + noMatches);
			write("Total invalid chars - not processed: " + invalidChars);
			System.out.println(  "for NAME");
			System.out.println(  "invalid " + invalidChars);
			System.out.println(  "matches " + matches);
			System.out.println(  "no matches " + noMatches);
			total = invalidChars + matches + noMatches;
			write("Total: " + total );
	
			write("Not Processes Due To (') Characters: " + invalidChars);
			write("------------------------");	
			write("------------------------");	
			for( int x = 0; x< invalidList.size(); x++ )
			{
				write((String)invalidList.get(x));
			}
			write("------------------------");
			write("------------------------");	
		
			write("------------------------");	
			write("No match found between NAME & NAME:");
			System.out.println( "NO MATCH 3");
			for( int x = 0; x< noMatch.size(); x++ )
			{
				write((String)noMatch.get(x));
			}
			write("------------------------");	
			write("DIIMG TAXID with Escape Characters: " + invalidChars);
			System.out.println( "NO PROCESS");
			for( int x = 0; x< invalidList.size(); x++ )
			{
				write((String)invalidList.get(x));
			}
			write("------------------------");
			write("Report Complete At: " + sdf.format(cal.getTime()));
            System.out.println("Report Complete At: " + sdf.format(cal.getTime()));
            System.out.println( "ALL DONE");
        }
        catch(Exception ex)
        {
        	System.out.println("error in runReport: " + ex.toString());
        }
        finally
        {
        	try
        	{
        		if(rs != null)          rs.close();
    			if(stmt != null)        stmt.close();    
        		if(connection != null)  connection.close();
        	}
        	catch(SQLException sqx)
        	{
        		System.out.println("Error closing connection: " + sqx.toString());
        	}         	
        }       
	}
	*/
	
	public static void write(String file, String msg) 
	{
		try 
        {
            FileWriter aWriter = new FileWriter(file, true);
            aWriter.write( msg + System.getProperty("line.separator"));
            aWriter.flush();
            aWriter.close();
        }
        catch (Exception e) 
        {
            System.out.println("error " + e.toString() );
        }
    }


}
