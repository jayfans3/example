package com.ailk.oci.ocnosql.client.rowkeygenerator;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class GenRKCallBackBJImpl implements GenRKCallBack {
	public String callback(String rowKey, String line) {
		StringBuffer buf = new StringBuffer(rowKey);

		StringBuffer lineBuf = new StringBuffer();
		String[] arr = line.split("\\t");
		lineBuf.append(arr[1]).append(arr[4]).append(arr[5]).append(arr[6])
				.append(arr[7]).append(arr[9]).append(arr[12]).append(arr[13])
				.append(arr[18]).append(arr[20]);

		return buf.append(Md5(lineBuf.toString())).toString();
	}

	private static String Md5(String plainText) {
		String result = null;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(plainText.getBytes());
			byte[] b = md.digest();

			StringBuffer buf = new StringBuffer("");
			for (int offset = 0; offset < b.length; offset++) {
				int i = b[offset];
				if (i < 0)
					i += 256;
				if (i < 16)
					buf.append("0");
				buf.append(Integer.toHexString(i));
			}
			result = buf.toString().substring(8, 24);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return result;
	}

	public static void main(String[] args) {
		String passwd = null;
		String loginpasswd = null;
		passwd = "a4daa2119aa542afb745934488ef0c23\t2984209763354551052\t13866599252\t0\t4166\t272\t0\t0\t0\t20140114042451\t213\t0\t220\t10417\t221438177\t221.177.5.177\tsohu.com\tusr.mb.hd.sohu.com\thttp://usr.mb.hd.sohu.com/getts.json\t213\tcmnet|2|1|2";
//		passwd = "a	b";
		loginpasswd = Md5(passwd);
//
//		passwd = "aa dd dsd dc d dd";
//		String[] arr = passwd.split("\\s");
//		System.out.println(arr.length);
//		StringBuffer lineBuf = new StringBuffer();
//		lineBuf.append(arr[1]).append(arr[3]).append(arr[4]);
//		System.out.println("MD5 16Bit : " + Md5(lineBuf.toString()));
		
		GenRKCallBackBJImpl impl = new GenRKCallBackBJImpl();
		System.out.println(impl.callback("", passwd));
	}

}