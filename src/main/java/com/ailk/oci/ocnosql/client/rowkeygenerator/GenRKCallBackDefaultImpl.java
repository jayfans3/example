package com.ailk.oci.ocnosql.client.rowkeygenerator;

import com.ailk.oci.ocnosql.client.config.spi.*;

import java.security.*;

/**
 * Created by IntelliJ IDEA.
 * User: lile3
 * Date: 13-11-13
 * Time: 下午3:39
 * To change this template use File | Settings | File Templates.
 */
public class GenRKCallBackDefaultImpl implements GenRKCallBack {

    @Override
    public String callback(String rowKey, String line) {
        StringBuffer buf = new StringBuffer(rowKey);
        return buf.append(Md5(line)).toString();
    }

    private static String Md5(String plainText) {
        String result = null;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(plainText.getBytes());
            byte b[] = md.digest();
            int i;
            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
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

    public static void main(String args[]) {
        String passwd = null;
        String loginpasswd = null;
        passwd = "rk;21;15810901114;man;bj;2013121109";   //密码明文
        loginpasswd = Md5(passwd);
        System.out.println("MD5 16Bit : " + loginpasswd);

        passwd = "uh;22;18910002225;woman;sh;20131210";   //密码明文
        loginpasswd = Md5(passwd);
        System.out.println("MD5 16Bit : " + loginpasswd);
    }
}
