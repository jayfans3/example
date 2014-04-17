package com.ailk.oci.ocnosql.client.rowkeygenerator;

public class HashRowKeyGenerator implements RowKeyGenerator{

    @Override
    public String generate(String oriRowKey, String needHashValue, String[] currenRowdata, int[] posIndex, String appendValue) {
        return null;
    }

    public Object generate(String oriRowKey) {
//		int h = oriRowKey.hashCode();
//		h ^= (h >>> 20) ^ (h >>> 12);  
//        h= h ^ (h >>> 7) ^ (h >>> 4);  
//	    String result = h+oriRowKey;
//	    return result;
    	return generatePrefix(oriRowKey) + oriRowKey;
	}

	@Override
	public Object generatePrefix(String oriRowKey) {
		int h = oriRowKey.hashCode();
		h ^= (h >>> 20) ^ (h >>> 12);  
        h= h ^ (h >>> 7) ^ (h >>> 4); 
		return h;
	}

}
