import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.modelmbean.ModelMBeanAttributeInfo;
import javax.management.modelmbean.ModelMBeanInfo;
import javax.management.modelmbean.ModelMBeanInfoSupport;
import javax.management.modelmbean.ModelMBeanOperationInfo;
import javax.management.modelmbean.RequiredModelMBean;
public class ModelMBeanUtils {
    private static final boolean READABLE = true;
    private static final boolean WRITABLE = true;
    private static final boolean BOOLEAN = true;
    private static final String STRING_CLASS = "java.lang.String"; 
    public static RequiredModelMBean createModlerMBean() {
        RequiredModelMBean model = null;
        try {
            model = new RequiredModelMBean();
            model.setManagedResource(new Hello(), "ObjectReference");
            ModelMBeanInfo info = createModelMBeanInfo();
            model.setModelMBeanInfo(info);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return model;
    } 
    private static ModelMBeanInfo createModelMBeanInfo() {
        //////////////////////////////////////////////////////////////////
        //                        属性                                        //
        //////////////////////////////////////////////////////////////////
        // 构造name属性信息
        ModelMBeanAttributeInfo nameAttrInfo = new ModelMBeanAttributeInfo(//
                "Name", // 属性名       
                STRING_CLASS, //属性类型    
                "people name", // 描述文字      
                READABLE, WRITABLE, !BOOLEAN, // 读写      
                null // 属性描述子
        ); 
        //////////////////////////////////////////////////////////////////
        //                        方法                                        //
        //////////////////////////////////////////////////////////////////
        //构造 printHello()操作的信息       
        ModelMBeanOperationInfo print1Info = new ModelMBeanOperationInfo(//
                "printHello", //
                null, //  
                null, //
                "void", //  
                MBeanOperationInfo.INFO, //    
                null //
        ); 
        // 构造printHello(String whoName)操作信息     
        ModelMBeanOperationInfo print2Info;
        MBeanParameterInfo[] param2 = new MBeanParameterInfo[1];
        param2[0] = new MBeanParameterInfo("whoName", STRING_CLASS, "say hello to who");
        print2Info = new ModelMBeanOperationInfo(//
                "printHello", //
                null,//
                param2,//
                "void", //
                MBeanOperationInfo.INFO, //
                null//
        ); 
        //////////////////////////////////////////////////////////////////
        //                        最后总合                                    //
        //////////////////////////////////////////////////////////////////
        // create ModelMBeanInfo       
        ModelMBeanInfo mbeanInfo = new ModelMBeanInfoSupport(//
                RequiredModelMBean.class.getName(), // MBean类
                null, // 描述文字     
                new ModelMBeanAttributeInfo[] { // 所有的属性信息（数组）
                nameAttrInfo },//只有一个属性
                null, // 所有的构造函数信息  
                new ModelMBeanOperationInfo[] { // 所有的操作信息（数组）
                        print1Info,
                        print2Info },//
                null, // 所有的通知信息(本例无)
                null//MBean描述子
        );
        return mbeanInfo;
} }
/**
 * 
 * 由于安全或其他原因，系统要把某个MBean公开的可管理方法隐藏起来。
 * 这时，如果你是用标准MBean，这需要修改接口类，然后重新编译发布；
 * 如果用Apache commons-modeler来写的模型MBean，
 * 则只需要修改XML文件就行了，不需要重新编译发布（可能要重启一下系统）。
 * 这就是模型Mbean优势之所在了。
 * 细心的人会发现动态MBean和这一节的模型Mbean非常相似，但它们还是有很大不同的：
 * 动态MBean没有Hello类，它要自己实现Hello类中的方法逻辑。 
 */
