

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

public class MovieMapper extends Mapper<Object, Movie, Text, NullWritable> { 

	 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();

    public void map(Object key, Movie movie, Context context) throws NullPointerException, IOException, InterruptedException {
    	// Format per tweet is id;date;hashtags;tweet;
    	 
//    	   String []arr = movieString.toString().split("::");
//    	   Movie movie = new Movie(arr[0],arr[1],arr[2]);
//    		data.set(movie.toString());
    	
    	   context.write(new Text(movie.getMovieId() + ":" + movie.getFilmName()), NullWritable.get());

    	
    	      	    
    }
    

    	
    
}