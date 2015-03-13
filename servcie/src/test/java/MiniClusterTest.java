/**
 * Created by liujs3 on 2015/1/15.
 */


import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.server.MiniYARNCluster;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class MiniClusterTest {

    public static void main(String[] args){
        Configuration configuration = new Configuration();
        try {
            File tempDir = Files.createTempDir();
            configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.getAbsolutePath());
            MiniDFSCluster miniDFSCluster=new MiniDFSCluster.Builder(configuration)
                    .numDataNodes(1)
                    .nameNodePort(9000)
                    .build();
            MiniYARNCluster a=new MiniYARNCluster("a", 1, 1, 1, 1,true);
//            configuration.set("yarn.resourcemanager.address", "ocean00:8032");
            a.serviceInit(configuration);
            a.init(configuration);
            a.start();
            a.getConfig().writeXml(new FileOutputStream(new File("conf.xml")));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
