

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class MovieReducer extends Reducer<Text, Text, Text, Text> {

    private IntWritable result = new IntWritable();
    private Text data = new Text();
    
    ArrayList<String> vals= new ArrayList<String>();
    protected void setup(Context context) throws IOException, InterruptedException {
    URI fileUri = context.getCacheFiles()[0];
    FileSystem fs = FileSystem.get(context.getConfiguration());
    FSDataInputStream in = fs.open( new Path(fileUri) );
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    String line = null;
    try {
    	
      br.readLine();
      while ((line = br.readLine()) != null) {
    	  String [] arr= line.split(" ");
    	  vals.add(line);
    	  
      }
      br.close();
     } catch (IOException e1) {
     }
   super.setup(context);
   }

    public void reduce(Text key, Iterable<Text> values, Context context)

              throws IOException, InterruptedException {

    	String preferedMovies = "";
        int sum = 0;

        for (Text movieId : values) {
        	preferedMovies += (movieId.toString() + ","); 
        	
  
        }
        preferedMovies = preferedMovies.substring(0, preferedMovies.length()-1);


 
            
               
               context.write(key, new Text(vals.toString()));
        
    }

}
