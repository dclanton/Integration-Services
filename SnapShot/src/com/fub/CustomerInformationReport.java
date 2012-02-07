package com.fub;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

public class CustomerInformationReport extends ImageGroup 
{

	private Connection connection;
	private ResultSet diimgSet;
	private ResultSet cfmastSet;
	private ResultSet countSet;
	private Statement diimgStmt;
	private Statement cfmastStmt; 
	private Statement countStmt; 

			
	public void build() 
	{
		System.out.println( "Building CIF...");
		log( "Building CIF...");
		ArrayList<String> list = new ArrayList<String>();
		String fileName = "C:\\FUBGlobal360_xmlexport_03_16_2010\\CIF_TID_0.txt";
		boolean empty = true;
		int count = 1;
		try
		{
      		connection = getConnection();
      		diimgStmt =  connection.createStatement(); 
			cfmastStmt = connection.createStatement();	
			countStmt = connection.createStatement();	
			countSet =   diimgStmt.executeQuery("SELECT count(*) AS ALLCOUNT from DATDUR.DIIMG WHERE DIOBJDAT <= '20100228' AND (DIAIDX01#) = '4' AND LENGTH(RTRIM(DIAIDX01V)) <> '0'");
			while(countSet.next())
			{
				log( "CIF Count : " + countSet.getString("ALLCOUNT")); 
			}			
			diimgSet =   diimgStmt.executeQuery("SELECT * from DATDUR.DIIMG WHERE DIOBJDAT <= '20100228' AND (DIAIDX01#) = '4' AND LENGTH(RTRIM(DIAIDX01V)) <> '0'"); 
			while (diimgSet.next())
			{
				count++;
				StringBuffer buffer = new StringBuffer();
				buffer.append("OBJECTNUM " +    			diimgSet.getString("DIOBJNO") + ", ");
				buffer.append("NUMSETS " +      			diimgSet.getString("DISETSEQ") + ", ");
				buffer.append("OBJSOURCE " +    			diimgSet.getString("DIOBJSIP")+ ", ");
				buffer.append("FROMUSER " +     			diimgSet.getString("DIOBJFUSR") + ", ");
				buffer.append("OBJTYPECODE " +  			diimgSet.getString("DIOBJTCD")+ ", ");
				buffer.append("LEADTYPETECH " + 			diimgSet.getString("DILEADTYP")+", ");
				buffer.append("STATUS " +       			diimgSet.getString("DIOBJSTS")+", ");
				buffer.append("GROUPROOTOBJ " + 			diimgSet.getString("DIGRROOT")+ ", ");
				buffer.append("SEQNUMINGROUP " +			diimgSet.getString("DIGRSEQ")+ ", ");
				buffer.append("EFFECTIVEDATE " +			diimgSet.getString("DIOBJEFF")+ ", ");
				buffer.append("OBJECTDATE " +   			diimgSet.getString("DIOBJDAT")+", ");
				buffer.append("OBJECTTIME " +   			diimgSet.getString("DIOBJTIM")+", ");
				buffer.append("OBJECTSIZE " +   			diimgSet.getString("DIOBJSIZ")+", ");
				buffer.append("ORIGFILENAME " + 			diimgSet.getString("DIORGFN")+", ");
				buffer.append("OBJEXTENSION " + 			diimgSet.getString("DIOBJEXT")+", ");
				buffer.append("VIEWED " +       			diimgSet.getString("DIVIEWED")+ ", ");
				buffer.append("DIRECTTONAME " + 			diimgSet.getString("DIDIRECT")+", ");
				buffer.append("DATEDIRECTED " + 			diimgSet.getString("DIDIRDAT")+", ");
				buffer.append("AUXDIRECTED " +  			diimgSet.getString("DIAUXD")+", ");
				buffer.append("DOCTYPE " +      			diimgSet.getString("DIDOC#")+", ");
				buffer.append("ANNOTATIONFLAG "+			diimgSet.getString("DIIMGANN")+ ", ");
				buffer.append("USAGECOUNTER " + 			diimgSet.getString("DIUSAGE")+", ");
				buffer.append("NUMERICFIELD1 " +			diimgSet.getString("DIFLD1B")+", ");
				buffer.append("NUMERICFIELD2 " +			diimgSet.getString("DIFLD2B")+", ");
				buffer.append("ALPHAFIELD1 " +  			diimgSet.getString("DIFLD1A")+", ");
				buffer.append("ALPHAFIELD2 " +  			diimgSet.getString("DIFLD2A")+ ", ");
				buffer.append("HIDEDATE " +    	 		    diimgSet.getString("DIHIDDAT")+", ");
				buffer.append("DELETEDATE " +   		    diimgSet.getString("DIDLTDAT")+", ");
				buffer.append("CURRENTLOCATION " + 	    	diimgSet.getString("DICURLOC")+", ");
				buffer.append("OPTICALRETENTION " +         diimgSet.getString("DI2ORGRP")+", ");
				buffer.append("PRIMARYVOLUME " +         	diimgSet.getString("DIOPTVOLP")+", ");
				buffer.append("BACKUPVOLUME " +          	diimgSet.getString("DIOPTVOLB")+", ");
				buffer.append("TIMESREQUIREDTOWRITE " + 	diimgSet.getString("DITIMROPT")+", ");
				buffer.append("TIMESWRITTENTOOPTICAL " + 	diimgSet.getString("DITIM2OPT")+", ");
				buffer.append("DATEIMAGEAREA " +         	diimgSet.getString("DIDT2IDSK")+", ");
				buffer.append("DATECACHEAREA " +         	diimgSet.getString("DIDT2CDSK")+", ");
				buffer.append("DATEWRITTENTOPRIMARY " +  	diimgSet.getString("DIDT2OPP")+", ");
				buffer.append("DATEWRITTENTOBACKUP " +   	diimgSet.getString("DIDT2OPB1")+", ");
				buffer.append("DATEWRITTENTO "  +        	diimgSet.getString("DIDT2OPB2")+", ");	
				String index = diimgSet.getString("DIAIDX01V").trim(); 
				String acctNo = index.length() == 10 ? index.substring(1, index.length()) : index;
				empty = true;
				cfmastSet = cfmastStmt.executeQuery("Select * from DATDUR.CFMAST WHERE CFSSNO ='" + acctNo  + "'" ); 
				while (cfmastSet.next())
				{
					empty = false;
					String tid = cfmastSet.getString("CFSSNO").trim();
					if(tid.equalsIgnoreCase("0"))
					{
						buffer.append("TAXID " +        tid+", ");
						buffer.append("CFFNA " +        cfmastSet.getString("CFFNA")+", ");
						buffer.append("CFMNA " +        cfmastSet.getString("CFMNA")+", ");
						buffer.append("CFLNA " +        cfmastSet.getString("CFLNA")+", ");
						buffer.append("CUSTNAMLNF " +  (cfmastSet.getString("CFSNME")).replaceAll("&", "&amp;")+", ");
						buffer.append("CUSTNAMFNF "+   (cfmastSet.getString("CFNA1")).replaceAll("&", "&amp;")+", ");
						buffer.append("CFCIFNBR " +     cfmastSet.getString("CFCIF#"));
						write(fileName, buffer.toString() + '\n');	
						System.out.println("Processed " + count);
					}
				}
				if(empty)
				{
					StringBuffer emptyBuffer = new StringBuffer();
					emptyBuffer.append("DIOBJNO; "   		 +  diimgSet.getString("DIOBJNO")   +", ");
					emptyBuffer.append("DIOBJFUSR; " 		 +  diimgSet.getString("DIOBJFUSR") +", ");
					emptyBuffer.append("DIDOC#; "    		 +  diimgSet.getString("DIDOC#")    +", ");
					emptyBuffer.append("DIOBJDAT; "  		 +  diimgSet.getString("DIOBJDAT")  +", ");						
					emptyBuffer.append("DIAIDX01V; " 		 +  diimgSet.getString("DIAIDX01V") +", ");
					emptyBuffer.append("DIAIDX02V; "		 +  diimgSet.getString("DIAIDX02V") +", ");
					emptyBuffer.append("DIAIDX03V; "		 +  diimgSet.getString("DIAIDX03V") +", ");
					emptyBuffer.append("DIAIDX04V; " 		 +  diimgSet.getString("DIAIDX04V")      );
					list.add(emptyBuffer.toString());
					continue;
				}					
			}				
			if(list.size() != 0)
			{
				fileName = "C:\\FUBGlobal360_xmlexport_03_16_2010\\no_match_log\\CIF.txt";
				String DATE_FORMAT_NOW = "yyyy-MM-dd HH:mm:ss";
				Calendar cal = Calendar.getInstance();
				SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
				write( fileName, sdf.format(cal.getTime() ));				
	        	for( int x=0; x< list.size(); x++)
	        	{
	        		write(fileName, list.get(x) + '\n'); 
	        	}
			}
		}
		catch(Exception ex)
		{
			System.out.println( "building error CIF " + ex.toString());
			log("building error CIF " + ex.toString());
		}
		finally
        {
			cleanUp();	    
        }
		System.out.println( "Build Complete CIF ");
		log( "Build Complete CIF ");
	}
	
	public void cleanUp()
    {
    	try
    	{
    		if(diimgStmt != null)   diimgStmt.close();    
			if(cfmastStmt != null)  cfmastStmt.close(); 
			if(countStmt != null)   countStmt.close(); 
								
    		if(diimgSet != null)    diimgSet.close();
    		if(cfmastSet != null)   cfmastSet.close();
    		if(countSet != null)    countSet.close();
    	   		
    		if(connection != null)  connection.close();
    	}
    	catch(Exception ex)
    	{
    		System.out.println("error closing connection CIF " + ex.toString());
    		log("error closing connection CIF " + ex.toString());
    	}
    }
}
