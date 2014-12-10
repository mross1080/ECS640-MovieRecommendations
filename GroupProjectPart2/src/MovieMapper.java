import java.util.Hashtable;
import java.util.ArrayList;
import java.util.Collections;
import java.net.URI;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;



public class MovieMapper extends Mapper<Object, Text, IntWritable, Text> {

    private String userID;
    private ArrayList<String> userList = new ArrayList<String>();
    private final Double JACCARD_COEFFICIENT_CONSTANT = 0.7; 

     @Override
    protected void setup(Context context) throws IOException, InterruptedException { 

    	URI fileUri = context.getCacheFiles()[0];

    	FileSystem fs = FileSystem.get(context.getConfiguration());
	FSDataInputStream in = fs.open( new Path(fileUri) );

    	BufferedReader br = new BufferedReader(new InputStreamReader(in));
	String line = null;

    	try {

      	     br.readLine();
             while ((line = br.readLine()) != null) {
        	String[] fields = line.split("\t");

        	if (fields.length == 2) {
 		           userID = fields[0];
                   StringTokenizer itr = new StringTokenizer(fields[1], ",");

        	    while (itr.hasMoreTokens()) {
         		 userList.add(itr.nextToken());
                    }

      		}
               }
     	       br.close();
      } catch (IOException e1) {
      }
   	super.setup(context);
   }  

    public void map(Object key, Text movies, Context context) throws IOException, InterruptedException {
     
    

    String dump = movies.toString();
    int matchCount = 0;
    StringBuffer nonMatchingRecord = new StringBuffer();

     StringTokenizer iter = new StringTokenizer(dump, ",");
     int size = iter.countTokens();
  
     while (iter.hasMoreTokens()) {
		String temp = iter.nextToken();
		matchCount += Collections.frequency(userList, temp);
             
		if ( !userList.contains(temp)){
        	nonMatchingRecord.append(temp + ",");
		}
     } 

    // Calucate the jaccard coefficient0 based on the intersection/union
    if  ( size != 0  && matchCount != 0)  {
            if (matchCount / size + userList.size() > JACCARD_COEFFICIENT_CONSTANT) {
              // emit the non matching records
              context.write( new IntWritable(Integer.parseInt(userID)) , new Text(nonMatchingRecord.substring(0, nonMatchingRecord.lastIndexOf(","))));
            }
    }


  }

 

}
