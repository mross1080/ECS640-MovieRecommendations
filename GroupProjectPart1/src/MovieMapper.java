

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class MovieMapper extends Mapper<Object, Text, Text, Text> { 

	 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();


    public void map(Object key, Text user, Context context) throws NullPointerException, IOException, InterruptedException {
    	
    		String [] fields = user.toString().split("::");
    		String movieId = fields[1];
    		String userId = fields[0];
    		
    		//If the movie was rated highly 
    		if(Float.parseFloat(fields[2]) > 4){
    			context.write(new Text(userId), new Text(movieId));
    		}
    			


    	
    	      	    
    }
    

    	
    
}