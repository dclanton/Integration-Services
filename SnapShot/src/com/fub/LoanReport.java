package com.fub;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

public class LoanReport extends ImageGroup 
{
	private Connection connection;
	private ResultSet diimgSet;
	private ResultSet cfmastSet;
	private ResultSet lnmastSet;
	private ResultSet countSet;
	
	private Statement diimgStmt;
	private Statement cfmastStmt; 
	private Statement lnmastStmt;
	private Statement countStmt; 
	
			
	public void build() 
	{
		System.out.println( "Building Loans...");
		log("Building Loans...");
		String fileName = "";
		ArrayList<String> list = new ArrayList<String>();
		boolean mismatch = true;
		boolean empty = true;
		int count =1;
		fileName = "C:\\FUBGlobal360_xmlexport_03_16_2010\\Type_L_TID_0.txt";
      	try
		{
      		connection = getConnection();
      		diimgStmt =  connection.createStatement(); 
			cfmastStmt = connection.createStatement();	
			lnmastStmt = connection.createStatement();	
			countStmt = connection.createStatement();	
			countSet =   diimgStmt.executeQuery("SELECT count(*) AS ALLCOUNT from DATDUR.DIIMG WHERE DIOBJDAT <= '20100228' AND SUBSTRING(DIAIDX01V, (LENGTH(RTRIM(DIAIDX01V))), 1) = 'L'");  
			while(countSet.next())
			{
				log( "Loans Count : " + countSet.getString("ALLCOUNT")); 
			}
			diimgSet =   diimgStmt.executeQuery("SELECT * from DATDUR.DIIMG WHERE DIOBJDAT <= '20100228' AND SUBSTRING(DIAIDX01V, (LENGTH(RTRIM(DIAIDX01V))), 1) = 'L'"); 
			while (diimgSet.next() )
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
				buffer.append("LOANNBR " +              	diimgSet.getString("DIAIDX01V")+", ");
				String index = diimgSet.getString("DIAIDX01V").trim(); 
				String acctNo = index.substring(0, (index.length()-1));
				empty = true;	
				mismatch = true;
				lnmastSet = lnmastStmt.executeQuery("SELECT CIFNO, COLCOD, TYPE FROM DATDUR.LNMAST WHERE ACCTNO = '" + acctNo + "'"); 
				outter:
				while (lnmastSet.next())
				{
					empty = false;
					buffer.append("LNMASTTYPE " +    lnmastSet.getString("TYPE")+", ");
					buffer.append("LNMASTCOLCOD " +  lnmastSet.getString("COLCOD")+", ");
					cfmastSet = cfmastStmt.executeQuery("Select * from DATDUR.CFMAST WHERE (CFCIF#) ='" + lnmastSet.getString("CIFNO").trim()  + "'" ); 
					while (cfmastSet.next())
					{
						  mismatch = false;
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
					if(mismatch)
					{
						StringBuffer mmbuffer = new StringBuffer();
						mmbuffer.append("DIOBJNO "   		 +  diimgSet.getString("DIOBJNO")   +", ");
						mmbuffer.append("DIOBJFUSR " 		 +  diimgSet.getString("DIOBJFUSR") +", ");
						mmbuffer.append("DIDOC# "    		 +  diimgSet.getString("DIDOC#")    +", ");
						mmbuffer.append("DIOBJDAT "  		 +  diimgSet.getString("DIOBJDAT")  +", ");						
						mmbuffer.append("DIAIDX01V " 		 +  diimgSet.getString("DIAIDX01V") +", ");
						mmbuffer.append("DIAIDX02V "		 +  diimgSet.getString("DIAIDX02V") +", ");
						mmbuffer.append("DIAIDX03V "		 +  diimgSet.getString("DIAIDX03V") +", ");
						mmbuffer.append("DIAIDX04V " 		 +  diimgSet.getString("DIAIDX04V") +", ");
						mmbuffer.append("TYPE "              +  lnmastSet.getString("TYPE")     +", ");
						mmbuffer.append("COLCOD "            +  lnmastSet.getString("COLCOD")   +", ");	
						mmbuffer.append("LNMAST CIFNO "      +  lnmastSet.getString("CIFNO")         );
						mmbuffer.append('\n');						
						empty = true;	
						write("C:\\FUBGlobal360_xmlexport_03_16_2010\\no_match_log\\Master_No_Match_L.txt", mmbuffer.toString() );	
					    break outter;
					}
				}
				if(empty)
				{
					 StringBuffer emptyBuffer = new StringBuffer();
					 emptyBuffer.append("DIOBJNO "   		 +  diimgSet.getString("DIOBJNO")   +", ");
					 emptyBuffer.append("DIOBJFUSR " 		 +  diimgSet.getString("DIOBJFUSR") +", ");
					 emptyBuffer.append("DIDOC# "    		 +  diimgSet.getString("DIDOC#")    +", ");
					 emptyBuffer.append("DIOBJDAT "  		 +  diimgSet.getString("DIOBJDAT")  +", ");						
					 emptyBuffer.append("DIAIDX01V " 		 +  diimgSet.getString("DIAIDX01V") +", ");
					 emptyBuffer.append("DIAIDX02V "		 +  diimgSet.getString("DIAIDX02V") +", ");
					 emptyBuffer.append("DIAIDX03V "		 +  diimgSet.getString("DIAIDX03V") +", ");
					 emptyBuffer.append("DIAIDX04V " 		 +  diimgSet.getString("DIAIDX04V")      );
					 list.add(emptyBuffer.toString());
					 continue;
				}
			}
			
			if(list.size() != 0)
			{
				fileName = "C:\\FUBGlobal360_xmlexport_03_16_2010\\no_match_log\\L.txt";
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
			log( "building error loans " + ex.toString());
		}
		finally
        {
			cleanUp();	    
        }
		System.out.println( "Build Loans Complete");
		log( "Build Loans Complete");
	}
	
	public void cleanUp()
    {
    	try
    	{
    		if(diimgStmt != null)   diimgStmt.close();    
			if(cfmastStmt != null)  cfmastStmt.close(); 
			if(lnmastStmt!= null)   lnmastStmt.close(); 
			if(countStmt != null)   countStmt.close(); 
			if(countSet != null)    countSet.close();
			
    		if(diimgSet != null)    diimgSet.close();
    		if(cfmastSet != null)   cfmastSet.close();
    		if(lnmastSet != null)   lnmastSet.close();
    		
    		if(connection != null)  connection.close();
    	}
    	catch(Exception ex)
    	{
    		System.out.println("error closing connectionloans " + ex.toString());
    		log("error closing connection loans " + ex.toString());
    	}
    }
}
