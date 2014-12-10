

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
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


public class MovieReducer extends Reducer<IntWritable, Text, Text, Text> {

    private IntWritable result = new IntWritable();
    private Text data = new Text();
    private Text message = new Text();
    private HashMap<String,String> movieNames;

    protected void setup(Context context) throws IOException, InterruptedException {
    	movieNames = new HashMap<String,String>();
        URI fileUri = context.getCacheFiles()[0];
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream in = fs.open( new Path(fileUri) );
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = null;
        try {
        	
          br.readLine();
          while ((line = br.readLine()) != null) {
        	  String [] arr= line.split(":");
        	  movieNames.put(arr[0], arr[1]);
        	  
          }
          br.close();
         } catch (IOException e1) {
         }
       super.setup(context);
       }
    
    public void reduce(IntWritable key, Iterable<Text> values, Context context)

              throws IOException, InterruptedException {
    	
    	ArrayList<String> finalRecommendations = new ArrayList<String>();
    	
    	
    	String [] movies;

        for (Text recs : values) {
        	movies= recs.toString().split(",");
        	for(String str: movies){
        		if(!finalRecommendations.contains(str)){
        			finalRecommendations.add(movieNames.get(str));
        		}
        	} 
        	
  
        }
        
        message.set("The movie recommendations for user " + key.get() + " are ");
        data.set(finalRecommendations.toString());
 
 
                    
               context.write(message, data);
        
    }

}
