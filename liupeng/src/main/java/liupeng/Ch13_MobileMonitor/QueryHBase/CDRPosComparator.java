package HBaseIndexAndQuery.QueryHBase;
import java.util.*;
public class CDRPosComparator implements Comparator {
	
	public int compare(Object o1 , Object o2)
	{
		CDRPosInFile pos1 = (CDRPosInFile) o1;
		CDRPosInFile pos2 = (CDRPosInFile) o2;
		
		if(pos1.fileID != pos2.fileID )
		{
			if( pos1.fileID > pos2.fileID)
			{
				return 1;
			}
			return -1;
		}
		
		if(pos1.offset > pos2.offset)
		{
			return 1;
		}
		return -1;

	}

}
