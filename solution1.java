import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.util.Random;
import java.io.InputStream;

public class solution1 {

   public static void main(String[] args) throws IOException {
    	// Instantiate a configuration 
	Configuration conf = new Configuration();
    // Take first argument 
    String uri = args[0];
    // Instantiate input stream
    FileSystem fsInput = FileSystem.get(URI.create(uri), conf);
    InputStream in = null;
    try {
        // set path for input
        Path inputPath = new Path(uri);
        // open input file
        in = fsInput.open(inputPath);
        // Instantiate a hdfs using the URI of an input directory
        FileSystem fsOutput = FileSystem.get(URI.create(args[1]), conf);
        // Instantiate a Path
        Path outFile = new Path(args[1]); 
        // Instantiate on output stream according to the
	   // output directory (path)
	   FSDataOutputStream out = fsOutput.create(outFile);
        // Create a buffer array to read bytes of data
        byte buffer[] = new byte[256];

        // Initialize a counter
        int bytesRead = 0;

        // Loop over the input stream until there is no
        // more data
        while( (bytesRead = in.read(buffer)) > 0) 
        // Write the content of buffer to output stream
        out.write(buffer, 0, bytesRead);
        // 
        // delete the original file
        fsInput.delete(inputPath, false); 
    } catch (IOException e) {
        e.printStackTrace();
    }
    finally {
        IOUtils.closeStream(in);
    }
   }
}

