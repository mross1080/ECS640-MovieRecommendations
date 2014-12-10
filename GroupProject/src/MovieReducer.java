

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MovieReducer extends Reducer<Movie, IntWritable, Movie, IntWritable> {

    private IntWritable result = new IntWritable();
    private Text data = new Text();

    public void reduce(Movie key, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {
        	sum += value.get();
  
        }

               result.set(sum);
//             data.set(key.toString());
            
               
               context.write(key, result);
        
    }

}
