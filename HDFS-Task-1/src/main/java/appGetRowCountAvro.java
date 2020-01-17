import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;

public class appGetRowCountAvro {
    public static void main(String[] args) throws IOException{
        String baseHDFSPath = "hdfs://sandbox-hdp.hortonworks.com:8020";
        String dirPath = "/user/root/dolany/a-downloads/";

        FileSystem fs = FileSystem.get(URI.create(baseHDFSPath), new Configuration());

        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(dirPath),  true);
        while (files.hasNext()) {
            LocatedFileStatus status = files.next();
            System.out.println(status.getPath());
        }

    }

}
