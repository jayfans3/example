package namenodezk1;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

public class exeShell implements Runnable{
	
	private String shellname;
	public exeShell(String shellname){
		this.shellname=shellname;
	}
	
	
	public void Exeshell(){
		
	
	try{
		Process process=Runtime.getRuntime().exec(shellname);
		InputStreamReader ir=new InputStreamReader(
				process.getInputStream());
		LineNumberReader input=new LineNumberReader(ir);
		String line;
		while((line=input.readLine())!=null)
			System.out.println(line);
		input.close();
		ir.close();
	}
	catch(Exception e){
		e.printStackTrace();
	}
	}

//     public  static void main(String []args){
//    	 
//	
//}


	@Override
	public void run() {
		// TODO Auto-generated method stub
		this.Exeshell();
		
	}

}
