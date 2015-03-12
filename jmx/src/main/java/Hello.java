import java.math.BigDecimal;

import org.apache.commons.lang.StringUtils;

public class Hello{
    private String name;
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public void printHello() {
        System.out.println("Hello World, " + name);
    }
    public void printHello(String whoName) {
        System.out.println("Hello , " + whoName);
    }
    public static void main(String args[]){
//    	BigDecimal b=BigDecimal.valueOf(1d);
//    	BigDecimal b3=new BigDecimal(0.1f);
//    	BigDecimal b4=new BigDecimal(-1f);
//    	BigDecimal b5=new BigDecimal("1");
//    	BigDecimal b6=new BigDecimal("-0.999999");
////    	BigDecimal b7=new BigDecimal("1");
////    	BigDecimal b8=new BigDecimal("-0.999999");
//    	
//    	BigDecimal b2=BigDecimal.valueOf(-0.999999d);
//    	System.out.println(b.add(b2));
//    	System.out.println(b3.add(b4));
//    	System.out.println(b5.add(b6));
//    	System.out.println(1d-0.99999d);
    	
    	
//    	System.out.println("asasdas adfa	addas	a 	".substring(0, "asasdas adfa	addas	a 	".length()-1));
    	StringBuffer temp2=new StringBuffer();
    	temp2.append("123456789\t");
    	temp2.deleteCharAt(temp2.length()-1);
    	System.out.println(StringUtils.isEmpty("\t"));
    }
} 