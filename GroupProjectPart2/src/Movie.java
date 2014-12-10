
import org.apache.hadoop.io.*;
import java.io.*;



public class Movie implements WritableComparable<Movie> {


	private Text movieId;
	private Text filmName;
	private Text genres;
	
	
	public Movie() {
	
		set(new Text(), new Text(), new Text());
	}


	public Movie(String movieId, String filmName, String genres) {
		set(new Text(movieId), new Text(filmName), new Text(genres));
	}

	public Movie(Text movieId, Text filmName, Text genres)
	{
		set(movieId, filmName, genres);

}


	public void set(Text movieId, Text filmName, Text genres) {
		this.movieId = movieId;
		this.filmName = filmName;
		this.genres = genres;
		
	}


	public Text getMovieId() {
		return movieId;
	}

	public Text getFilmName() {
		return filmName;
	}

	public Text getGenres() {
		return genres;
	}

//	@Override
	public void write(DataOutput out) throws IOException {
		movieId.write(out);
		filmName.write(out);
		genres.write(out);
		
	}


//	@Override
	public void readFields(DataInput in) throws IOException {
		movieId.readFields(in);
		filmName.readFields(in);
		genres.readFields(in);
		
	}

	public int compareTo(Movie st) {
		
		
		Integer.compare(Integer.parseInt(movieId.toString()), Integer.parseInt(st.getMovieId().toString()));
		int cmp = Integer.compare(Integer.parseInt(movieId.toString()), Integer.parseInt(st.getMovieId().toString()));
	      if (cmp != 0) {
	         return cmp;
	      }
	      
	      return 0;
		
//		if(Integer.parseInt(movieId.toString()) > Integer.parseInt(st.getMovieId().toString())){
//			return 10;
//			
//		} else if(Integer.parseInt(movieId.toString()) < Integer.parseInt(st.getMovieId().toString())){
//			return -10;
//		} else{
//			return 0;
//		}


	}

	//@Override
	public String toString() {

		return "["+ movieId + "," + filmName + "," + genres + "]";
	}



}


