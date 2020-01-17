import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import org.apache.avro.mapred.FsInput;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class appGetSchemaAvroFile {
    public static void main(String[] args) throws IOException{
        String baseHDFSPath = "hdfs://sandbox-hdp.hortonworks.com:8020";
        String dirPath = "/user/root/dolany/a-downloads/";

        Configuration mconf = new Configuration();
        Path mpath = new Path(dirPath);

        FileSystem fs = FileSystem.get(URI.create(baseHDFSPath),mconf);
        FileStatus[] files = fs.listStatus(mpath);

        for (FileStatus file : files) {

            if (file.getPath().toString().contains(".avro"))
            {
                DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
                String filePath = ""+file.getPath();
                System.out.println(filePath);

                Path mfpath = new Path(String.valueOf(file.getPath()));
                System.out.println("Initiated: "+mfpath);

                FsInput fsInputStream = new FsInput(mfpath, mconf);

                if (fsInputStream != null) { System.out.println("fsInputStream is initiated");}

                DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(fsInputStream, datumReader);

                if (dataFileReader != null) { System.out.println("dataFileReader is initiated");}

                Schema schema = dataFileReader.getSchema();
                System.out.println(schema);
                //System.out.println(status.getPath());
            }
        }

    }

}