import java.util.StringTokenizer;


/**
 * 
 */

/**
 * @author liujs3
 *
 */
public class tocken {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String value ="a	0	c	d";
		StringTokenizer itr = new StringTokenizer(value.toString());
		while(itr.hasMoreTokens())
	    	  if(itr.nextToken("1")=="0"){
	    		  System.out.println("ok");
	    	  }
	      }
	}

