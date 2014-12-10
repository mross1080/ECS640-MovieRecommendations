


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;


public class MovieInputFormat extends FileInputFormat<Text,Movie> {


	public RecordReader<Text, Movie> createRecordReader(
			InputSplit inputSplit, TaskAttemptContext context) {
		return new MovieRecordReader();
	}

	/**
	 * Modified LineRecordReader (default one automatically parsing the
	 * StockInput data)
	 * 
	 * @author fcuadrado
	 * 
	 */

	public class MovieRecordReader extends RecordReader<Text, Movie> {
	
		private CompressionCodecFactory compressionCodecs = null;
		private long start;
		private long pos;
		private long end;
		
		private LineReader in;
		private int maxLineLength;

		private Text key = null;
		private Movie value = null;

		private Text line = new Text();

		// internal fields for daily stock


		private Text genre = new Text();
		private Text movieID = new Text();
		private Text title = new Text();

		public void initialize(InputSplit genericSplit,
				TaskAttemptContext context) throws IOException {
			FileSplit split = (FileSplit) genericSplit;
			Configuration job = context.getConfiguration();
			this.maxLineLength = job.getInt(
					"mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
			start = split.getStart();
			end = start + split.getLength();
			final Path file = split.getPath();
			compressionCodecs = new CompressionCodecFactory(job);
			final CompressionCodec codec = compressionCodecs.getCodec(file);

			// open the file and seek to the start of the split
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(split.getPath());
			boolean skipFirstLine = false;
			if (codec != null) {
				in = new LineReader(codec.createInputStream(fileIn), job);
				end = Long.MAX_VALUE;
			} else {
				if (start != 0) {
					skipFirstLine = true;
					--start;
					fileIn.seek(start);
				}
				in = new LineReader(fileIn, job);
			}
			if (skipFirstLine) { // skip first line and re-establish "start".
				start += in.readLine(new Text(), 0,
						(int) Math.min((long) Integer.MAX_VALUE, end - start));
			}
			this.pos = start;
		}

		public boolean nextKeyValue() throws IOException {
			if (key == null) {
				key = new Text();
			}

			if (value == null) {
				value = new Movie();
			}
			int newSize = 0;

			while (pos < end) {
				newSize = in.readLine(line, maxLineLength, Math.max(
						(int) Math.min(Integer.MAX_VALUE, end - pos),
						maxLineLength));
				if (newSize == 0) {
					break;
				}

				// fields:
				// movieID::Title::Genre
				String[] fields = line.toString().split("::");

				// data must be correctly formed
				if (fields == null || fields.length != 3) {
					break;
				}
				// parse key
				// key is the first field, pointing the movie id
				key.set(fields[0]);

				title.set(fields[1]);


				// setting day by parsing from
				try {
					genre.set(fields[2]);
					

					value.set(key, title, genre);


				} catch (NumberFormatException e) {
					break;
				}
				//

				pos += newSize;
				if (newSize < maxLineLength) {
					break;
				}

				}
			if (newSize == 0) {
				key = null;
				value = null;
				return false;
			} else {
				return true;
			}
		}

		@Override
		public Text getCurrentKey() {
			return key;
		}

		@Override
		public Movie getCurrentValue() {
			return value;
		}

		/**
		 * Get the progress within the split
		 */
		public float getProgress() {
			if (start == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (pos - start) / (float) (end - start));
			}
		}

		public synchronized void close() throws IOException {
			if (in != null) {
				in.close();
			}
		}
	}
	
	@Override
	  protected boolean isSplitable(JobContext context, Path file) {
	    CompressionCodec codec = 
	      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
	    return codec == null;
	  }

}