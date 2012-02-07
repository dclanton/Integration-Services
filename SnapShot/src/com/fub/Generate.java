package com.fub;

public class Generate 
{	
	public static void main(String[] args) 
    {
		CIF cif = new CIF();
		cif.build();
		/*
       	for( int x=0; x< args.length; x++)
    	{
       		switch(args[x].charAt(0))
    		{
    			case 'a':   AccountsPayable ap = new AccountsPayable();
    	    			    ap.build();
    	    			    break;
    			case 'b':   SafteyDeposit sd = new SafteyDeposit();
    		    			sd.build();
    		    			break;
    			case 'd':
    			case 's': 	Deposits d = new Deposits();
    		    			d.build();
    		    			break;
    			case 't': 	TimeDeposits t = new TimeDeposits();
    		    			t.build();
    		    			break;
    			case 'l': 	Loans l = new Loans();
    		    			l.build(); 
    		    			break;	
    			case 'f': 	CustomerInformation cf = new CustomerInformation();
    						cf.build();
    						break;
    			case 'r': 	Reports r = new Reports();
    		    			r.build();
    		    			break;
    			case '*':   ap = new AccountsPayable();
    	    				ap.build();
    	    				sd = new SafteyDeposit();
    	    				sd.build();
    		    		    d = new Deposits();
    		                d.build();
    		    		    t = new TimeDeposits();
    		                t.build();	    		    
    		                l = new Loans();
    		                l.build();	    		    
    		                cf = new CustomerInformation();
    		                cf.build();
    		    		    r = new Reports();
    		                r.build();
    		                break;
    	  }
       }
       */
   }
}
