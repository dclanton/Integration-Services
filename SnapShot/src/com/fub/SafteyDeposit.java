package com.fub;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;


public class SafteyDeposit extends ImageGroup 
{
	private Connection connection;
	private ResultSet diimgSet;
	private ResultSet cfmastSet;
	private ResultSet sdmastSet;
	private ResultSet countSet;
	
	private Statement diimgStmt;
	private Statement cfmastStmt; 
	private Statement sdmastStmt; 
	private Statement countStmt; 
	
			
	public void build() 
	{
		System.out.println( "Building Saftey Deposits...");
		log("Building Saftey Deposits...");
		ArrayList<String> list = new ArrayList<String>();
		String fileName = "";
		int count = 0;
		int dirNum = 1;
		boolean empty = true;
		boolean mismatch = true;
      	try
		{
      		connection = getConnection();
      		diimgStmt =  connection.createStatement(); 
			cfmastStmt = connection.createStatement();	
			sdmastStmt = connection.createStatement();	
			countStmt = connection.createStatement();	
			countSet =   diimgStmt.executeQuery("SELECT count(*) AS ALLCOUNT from DATDUR.DIIMG WHERE DIOBJDAT >= '20100301'  AND SUBSTRING(DIAIDX01V, (LENGTH(RTRIM(DIAIDX01V))), 1) = 'B'"); 
			while(countSet.next())
			{
				log( "Saftey Deposits Count : " + countSet.getString("ALLCOUNT")); 
			}
			diimgSet =   diimgStmt.executeQuery("SELECT * from DATDUR.DIIMG WHERE DIOBJDAT >= '20100301' AND SUBSTRING(DIAIDX01V, (LENGTH(RTRIM(DIAIDX01V))), 1) = 'B'"); 
			upper:
			while (diimgSet.next()  )
			{
				count++;
				StringBuffer buffer = new StringBuffer();
				buffer.append("<row xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">");
				buffer.append("<OBJECTNUM>" +    			diimgSet.getString("DIOBJNO") + "</OBJECTNUM>");
				buffer.append("<NUMSETS>" +      			diimgSet.getString("DISETSEQ") + "</NUMSETS>");
				buffer.append("<OBJSOURCE>" +    			diimgSet.getString("DIOBJSIP")+ "</OBJSOURCE>");
				buffer.append("<FROMUSER>" +     			diimgSet.getString("DIOBJFUSR") + "</FROMUSER>");
				buffer.append("<OBJTYPECODE>" +  			diimgSet.getString("DIOBJTCD")+ "</OBJTYPECODE>");
				buffer.append("<LEADTYPETECH>" + 			diimgSet.getString("DILEADTYP")+"</LEADTYPETECH>");
				buffer.append("<STATUS>" +       			diimgSet.getString("DIOBJSTS")+"</STATUS>");
				buffer.append("<GROUPROOTOBJ>" + 			diimgSet.getString("DIGRROOT")+ "</GROUPROOTOBJ>");
				buffer.append("<SEQNUMINGROUP>" +			diimgSet.getString("DIGRSEQ")+ "</SEQNUMINGROUP>");
				buffer.append("<EFFECTIVEDATE>" +			diimgSet.getString("DIOBJEFF")+ "</EFFECTIVEDATE>");
				buffer.append("<OBJECTDATE>" +   			diimgSet.getString("DIOBJDAT")+"</OBJECTDATE>");
				buffer.append("<OBJECTTIME>" +   			diimgSet.getString("DIOBJTIM")+"</OBJECTTIME>");
				buffer.append("<OBJECTSIZE>" +   			diimgSet.getString("DIOBJSIZ")+"</OBJECTSIZE>");
				buffer.append("<ORIGFILENAME>" + 			diimgSet.getString("DIORGFN")+"</ORIGFILENAME>");
				buffer.append("<OBJEXTENSION>" + 			diimgSet.getString("DIOBJEXT")+"</OBJEXTENSION>");
				buffer.append("<VIEWED>" +       			diimgSet.getString("DIVIEWED")+ "</VIEWED>");
				buffer.append("<DIRECTTONAME>" + 			diimgSet.getString("DIDIRECT")+"</DIRECTTONAME>");
				buffer.append("<DATEDIRECTED>" + 			diimgSet.getString("DIDIRDAT")+"</DATEDIRECTED>");
				buffer.append("<AUXDIRECTED>" +  			diimgSet.getString("DIAUXD")+"</AUXDIRECTED>");
				buffer.append("<DOCTYPE>" +      			diimgSet.getString("DIDOC#")+"</DOCTYPE>");
				buffer.append("<ANNOTATIONFLAG>"+			diimgSet.getString("DIIMGANN")+ "</ANNOTATIONFLAG>");
				buffer.append("<USAGECOUNTER>" + 			diimgSet.getString("DIUSAGE")+"</USAGECOUNTER>");
				buffer.append("<NUMERICFIELD1>" +			diimgSet.getString("DIFLD1B")+"</NUMERICFIELD1>");
				buffer.append("<NUMERICFIELD2>" +			diimgSet.getString("DIFLD2B")+"</NUMERICFIELD2>");
				buffer.append("<ALPHAFIELD1>" +  			diimgSet.getString("DIFLD1A")+"</ALPHAFIELD1>");
				buffer.append("<ALPHAFIELD2>" +  			diimgSet.getString("DIFLD2A")+ "</ALPHAFIELD2>");
				buffer.append("<HIDEDATE>" +    	 		diimgSet.getString("DIHIDDAT")+"</HIDEDATE>");
				buffer.append("<DELETEDATE>" +   		    diimgSet.getString("DIDLTDAT")+"</DELETEDATE>");
				buffer.append("<CURRENTLOCATION>" + 	    diimgSet.getString("DICURLOC")+"</CURRENTLOCATION>");
				buffer.append("<OPTICALRETENTION>" +        diimgSet.getString("DI2ORGRP")+"</OPTICALRETENTION>");
				buffer.append("<PRIMARYVOLUME>" +           diimgSet.getString("DIOPTVOLP")+"</PRIMARYVOLUME>");
				buffer.append("<BACKUPVOLUME>" +            diimgSet.getString("DIOPTVOLB")+"</BACKUPVOLUME>");
				buffer.append("<TIMESREQUIREDTOWRITE>" +    diimgSet.getString("DITIMROPT")+"</TIMESREQUIREDTOWRITE>");
				buffer.append("<TIMESWRITTENTOOPTICAL>" +   diimgSet.getString("DITIM2OPT")+"</TIMESWRITTENTOOPTICAL>");
				buffer.append("<DATEIMAGEAREA>" +           diimgSet.getString("DIDT2IDSK")+"</DATEIMAGEAREA>");
				buffer.append("<DATECACHEAREA>" +           diimgSet.getString("DIDT2CDSK")+"</DATECACHEAREA>");
				buffer.append("<DATEWRITTENTOPRIMARY>" +    diimgSet.getString("DIDT2OPP")+"</DATEWRITTENTOPRIMARY>");
				buffer.append("<DATEWRITTENTOBACKUP>" +     diimgSet.getString("DIDT2OPB1")+"</DATEWRITTENTOBACKUP>");
				buffer.append("<DATEWRITTENTO>"  +          diimgSet.getString("DIDT2OPB2")+"</DATEWRITTENTO>");
				buffer.append("<LOANNBR>" +                diimgSet.getString("DIAIDX01V")+"</LOANNBR>");
				String index = diimgSet.getString("DIAIDX01V").trim(); 
				String val = index.substring(0, (index.length()-1));
				empty = true;
				mismatch = true;
				//if( (diimgSet.getString("DIAIDX02V").trim()).length() == 0 )
				//{
				//	count--;
				//	continue;
				//}
				sdmastSet = sdmastStmt.executeQuery("Select CIFNO, PRIME from DATDUR.SDMAST WHERE ACCTNO ='" + val + "'" );
				outter:
				while (sdmastSet.next())
				{
					empty = false;
					buffer.append("<PRIME>" + sdmastSet.getString("PRIME")+"</PRIME>");
					if( (sdmastSet.getString("CIFNO").trim()).length() == 0 &&
						(diimgSet.getString("DIAIDX02V").trim()).length() != 0)
					{
						cfmastSet = cfmastStmt.executeQuery("Select * from DATDUR.CFMAST WHERE CFSSNO ='" + diimgSet.getString("DIAIDX02V").trim() + "'" ); 
					}
					else
					{
						//count--;
						//continue upper;
					    cfmastSet = cfmastStmt.executeQuery("Select * from DATDUR.CFMAST WHERE (CFCIF#) ='" + sdmastSet.getString("CIFNO").trim()  + "'" ); 
					}
					while (cfmastSet.next())
					{
						mismatch = false;
						String tid = cfmastSet.getString("CFSSNO").trim();
						/*
						if( tid.length() < 9)
						{
							  switch(tid.length())
							  {
							     case 1: tid = "00000000" + tid;
							    	     break;
							     case 2: tid = "0000000" + tid;
							    	 	 break;
							     case 3: tid = "000000" + tid;
							    	 	 break;
							     case 4: tid = "00000" + tid;
							    	 	 break;
							     case 5: tid = "0000" + tid;
							    	 	 break;
							     case 6: tid = "000" + tid;
							    	 	 break;
							     case 7: tid = "00" + tid;
							    	 	 break;
							     case 8: tid = "0" + tid;
							    	 	 break;
							  }
						  }	
						  */					 
						buffer.append("<TAXID>" +        tid+"</TAXID>");
						buffer.append("<CFFNA>" +        cfmastSet.getString("CFFNA")+"</CFFNA>");
						buffer.append("<CFMNA>" +        cfmastSet.getString("CFMNA")+"</CFMNA>");
						buffer.append("<CFLNA>" +        cfmastSet.getString("CFLNA")+"</CFLNA>");
						buffer.append("<CUSTNAMLNF>" +  (cfmastSet.getString("CFSNME")).replaceAll("&", "&amp;")+"</CUSTNAMLNF>");
						buffer.append("<CUSTNAMFNF>"+   (cfmastSet.getString("CFNA1")).replaceAll("&", "&amp;")+"</CUSTNAMFNF>");
						buffer.append("<CFCIFNBR>" +     cfmastSet.getString("CFCIF#")+"</CFCIFNBR>");
					}	
					if(mismatch)
					{
						StringBuffer mmbuffer = new StringBuffer();
						mmbuffer.append("DIOBJNO; "   		 +  diimgSet.getString("DIOBJNO")   +"; ");
						mmbuffer.append("DIOBJFUSR; " 		 +  diimgSet.getString("DIOBJFUSR") +"; ");
						mmbuffer.append("DIDOC#; "    		 +  diimgSet.getString("DIDOC#")    +"; ");
						mmbuffer.append("DIOBJDAT; "  		 +  diimgSet.getString("DIOBJDAT")  +"; ");						
						mmbuffer.append("DIAIDX01V; " 		 +  diimgSet.getString("DIAIDX01V") +"; ");
						mmbuffer.append("DIAIDX02V; "		 +  diimgSet.getString("DIAIDX02V") +"; ");
						mmbuffer.append("DIAIDX03V; "		 +  diimgSet.getString("DIAIDX03V") +"; ");
						mmbuffer.append("DIAIDX04V; " 		 +  diimgSet.getString("DIAIDX04V") +"; ");
						mmbuffer.append("PRIME; " 		     +  sdmastSet.getString("PRIME")    +"; ");
						mmbuffer.append("SDMAST CIFNO;"      +  sdmastSet.getString("CIFNO")    +"; ");
						mmbuffer.append('\n');						
						empty = true;	
						write("C:\\FUBGlobal360_xmlexport\\no_match_log\\Master_No_Match_B.txt", mmbuffer.toString() );	
					    break outter;
					}
				}
				if(empty)
				{
					count--;
					StringBuffer emptyBuffer = new StringBuffer();
					emptyBuffer.append("DIOBJNO; "   		 +  diimgSet.getString("DIOBJNO")   +"; ");
					emptyBuffer.append("DIOBJFUSR; " 		 +  diimgSet.getString("DIOBJFUSR") +"; ");
					emptyBuffer.append("DIDOC#; "    		 +  diimgSet.getString("DIDOC#")    +"; ");
					emptyBuffer.append("DIOBJDAT; "  		 +  diimgSet.getString("DIOBJDAT")  +"; ");						
					emptyBuffer.append("DIAIDX01V; " 		 +  diimgSet.getString("DIAIDX01V") +"; ");
					emptyBuffer.append("DIAIDX02V; "		 +  diimgSet.getString("DIAIDX02V") +"; ");
					emptyBuffer.append("DIAIDX03V; "		 +  diimgSet.getString("DIAIDX03V") +"; ");
					emptyBuffer.append("DIAIDX04V; " 		 +  diimgSet.getString("DIAIDX04V") +"; ");
					list.add(emptyBuffer.toString());
					continue;				
				}				
				buffer.append("</row>");
				if(count == 1)
				{
					File theDir = new File( "C:\\FUBGlobal360_xmlexport\\Type B\\set" + dirNum);
					theDir.mkdir();
					fileName = "C:\\FUBGlobal360_xmlexport\\Type B\\set" + dirNum + "\\" + diimgSet.getString("DIOBJNO") + ".xml";
				}
				else if( count < 50000)
				{
					fileName = "C:\\FUBGlobal360_xmlexport\\Type B\\set" + dirNum + "\\" + diimgSet.getString("DIOBJNO") + ".xml";
				}
				else
				{
					fileName = "C:\\FUBGlobal360_xmlexport\\Type B\\set" + dirNum + "\\" + diimgSet.getString("DIOBJNO") + ".xml";
					count = 0;
					dirNum++;
				}
				write(fileName, buffer.toString());				
			}
			
			if(list.size() != 0)
			{
				fileName = "C:\\FUBGlobal360_xmlexport\\no_match_log\\B.txt";
				String DATE_FORMAT_NOW = "yyyy-MM-dd HH:mm:ss";
				Calendar cal = Calendar.getInstance();
				SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
				write( fileName, sdf.format(cal.getTime() ) + '\n');
		       	for( int x=0; x< list.size(); x++)
	        	{
	        		write(fileName, list.get(x) + '\n'); 
	        	}
			}
		}
		catch(Exception ex)
		{
			System.out.println( "building error " + ex.toString());
			log( "building error SafteyDeposits" + ex.toString());
		}
		finally
        {
			cleanUp();	    
        }
		System.out.println( "Building Saftey Deposits Complete");
		log( "Building Saftey Deposits Complete");
	}
	
	public void cleanUp()
    {
    	try
    	{
    		if(diimgStmt != null)   diimgStmt.close();    
			if(cfmastStmt != null)  cfmastStmt.close(); 
			if(sdmastStmt!= null)   sdmastStmt.close(); 
			if(countStmt != null)   countStmt.close(); 
			if(countSet != null)    countSet.close();
			
    		if(diimgSet != null)    diimgSet.close();
    		if(cfmastSet != null)   cfmastSet.close();
    		if(sdmastSet != null)   sdmastSet.close();
    		
    		if(connection != null)  connection.close();
    	}
    	catch(Exception ex)
    	{
    		System.out.println("error closing connection " + ex.toString());
    		log("error closing connection SafteyDeposits " + ex.toString());
    	}
    }
}
