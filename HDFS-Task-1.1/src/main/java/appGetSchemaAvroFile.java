import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class appGetSchemaAvroFile {
    public static void main(String[] args) throws IOException {

        // https://creativedata.atlassian.net/wiki/spaces/SAP/pages/52199514/Java+-+Read+Write+files+with+HDFS
        String baseHdfsPath = "hdfs://sandbox-hdp.hortonworks.com:8020";
        String dirPath = "/user/root/dolany/a-downloads/";
        FileSystem fs = FileSystem.get(URI.create(baseHdfsPath), new Configuration());


        HdfsFilesIterator filesIter = new HdfsFilesIterator(fs.listFiles(new Path(dirPath), true));
        Spliterator<LocatedFileStatus> spliterator = Spliterators.spliteratorUnknownSize(filesIter, 0);
        Stream<LocatedFileStatus> files = StreamSupport.stream(spliterator, false);

        List<Path> avroFiles = files.map(FileStatus::getPath)
                .filter(p -> p.toString().contains(".avro"))
                .collect(Collectors.toList());

        Path firstAvro = avroFiles.stream()
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No files was found"));

        System.out.printf("Avro file found: %s%n", firstAvro.toString());
        // hdfs://sandbox-hdp.hortonworks.com:8020/user/root/data/data/expedia/part-00000-ef2b800c-0702-462d-b37f-5f2fb3a093d0-c000.avro

        System.out.printf("Avro file schema: %s%n", readAvroSchema(fs, firstAvro));
        System.out.printf("Avro Record: %s%n", readAvroFile(fs, firstAvro));
        System.out.printf("Avro Records Count: %s%n", countLines(fs, firstAvro));


        System.out.printf("Total Avro Records Count: %s%n", avroFiles.stream().mapToLong(p -> countLines(fs, p)).sum());
    }

    private static <T> T processAvro(FileSystem fs, Path avroFile, Function<DataFileReader<GenericRecord>, T> f) {
        try {
            final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            FsInput in = new FsInput(avroFile, fs.getConf());
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(in, datumReader);
            return f.apply(dataFileReader);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Schema readAvroSchema(FileSystem fs, Path avroFile) {
        return processAvro(fs, avroFile, DataFileStream::getSchema);
    }

    private static GenericRecord readAvroFile(FileSystem fs, Path avroFile) {
        return processAvro(fs, avroFile, DataFileStream::next);
    }

    private static Long countLines(FileSystem fs, Path avroFile) {
        return processAvro(fs, avroFile, reader -> StreamSupport
                .stream(reader.spliterator(), false)
                .count());
    }

    private static class HdfsFilesIterator implements Iterator<LocatedFileStatus> {
        private final RemoteIterator<LocatedFileStatus> iter;

        public HdfsFilesIterator(RemoteIterator<LocatedFileStatus> iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            try {
                return this.iter.hasNext();
            } catch (IOException e) {
                e.printStackTrace();
                //TODO: Need to handle gracefully
            }
            return false;
        }

        @Override
        public LocatedFileStatus next() {
            try {
                return this.iter.next();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static <T> Stream<T> getStreamFromIterator(Iterator<T> iterator) {
        // Convert the iterator to Spliterator
        Spliterator<T>
                spliterator = Spliterators
                .spliteratorUnknownSize(iterator, 0);
        // Get a Sequential Stream from spliterator
        return StreamSupport.stream(spliterator, false);
    }
}
