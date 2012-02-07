package com.fub;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;


public class Reports extends ImageGroup 
{
	private Connection connection;
	private ResultSet diimgSet;
	private ResultSet countSet;
	private Statement diimgStmt;
	private Statement countStmt; 
			
	public void build() 
	{
		System.out.println( "Building Reports...");
		log( "Building Reports...");
		String fileName = "";
		int count = 0;
		int dirNum = 1;
	   	try
		{
      		connection = getConnection();
      		diimgStmt =  connection.createStatement(); 
      		countStmt = connection.createStatement();	
			countSet =   diimgStmt.executeQuery("SELECT count(*) AS ALLCOUNT from DATDUR.DIIMG WHERE DIOBJDAT >= '20100301' AND (DIAIDX01#) = '4' AND LENGTH(RTRIM(DIAIDX01V)) = '0'"); 
			while(countSet.next())
			{
				log( "Reports Count : " + countSet.getString("ALLCOUNT")); 
			}
			diimgSet =   diimgStmt.executeQuery("SELECT * from DATDUR.DIIMG WHERE DIOBJDAT >= '20100301' AND (DIAIDX01#) = '4' AND LENGTH(RTRIM(DIAIDX01V)) = '0'"); 
			while (diimgSet.next())
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
				buffer.append("</row>");
				if(count == 1)
				{
					File theDir = new File( "C:\\FUBGlobal360_xmlexport\\Reports\\set" + dirNum);
					theDir.mkdir();
					fileName = "C:\\FUBGlobal360_xmlexport\\Reports\\set" + dirNum + "\\" + diimgSet.getString("DIOBJNO") + ".xml";
				}
				else if( count < 50000)
				{
					fileName = "C:\\FUBGlobal360_xmlexport\\Reports\\set" + dirNum + "\\" + diimgSet.getString("DIOBJNO") + ".xml";
				}
				else
				{
					fileName = "C:\\FUBGlobal360_xmlexport\\Reports\\set" + dirNum + "\\" + diimgSet.getString("DIOBJNO") + ".xml";
					count = 0;
					dirNum++;
				}
				write(fileName, buffer.toString());				
			}				
		}
		catch(Exception ex)
		{
			System.out.println( "building error Reports " + ex.toString());
			log("building error Reports " + ex.toString());
		}
		finally
        {
			cleanUp();	    
        }
		System.out.println( "Build Complete Reports");
		log( "Build Complete Reports");
	}
	
	public void cleanUp()
    {
    	try
    	{
    		if(diimgStmt != null)   diimgStmt.close();    
			if(diimgSet != null)    diimgSet.close();    
			if(countStmt != null)   countStmt.close(); 
			if(countSet != null)    countSet.close();
    		if(connection != null)  connection.close();
    	}
    	catch(Exception ex)
    	{
    		System.out.println("error closing connection Reports " + ex.toString());
    		log("error closing connection Reports " + ex.toString());
    	}
    }
}
