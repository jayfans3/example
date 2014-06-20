import com.ailk.cloudetl.commons.utils.VelocityUtil;


public class Trap {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String shell = VelocityUtil.getInstance().evaluate(udfParams, cmd);
	}

}
