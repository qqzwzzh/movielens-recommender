import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RowMPreReduce extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	private Text valText = new Text();

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		try {

			// Input: (userid, List<movieid, rating>)

			float sum = 0.0F;
			int totalRatingCount = 0;

			ArrayList<String> movieID = new ArrayList<String>();
			ArrayList<Float> rating = new ArrayList<Float>();

			while (values.hasNext()) {
				String[] movieRatingPair = values.next().toString().split(",");
				movieID.add(movieRatingPair[0]);
				Float parseRating = Float.parseFloat(movieRatingPair[1]);
				rating.add(parseRating);

				sum += parseRating;
				totalRatingCount++;
			}

			float average = ((float) sum) / totalRatingCount;

			for (int i = 0; i < movieID.size(); i++) {
				valText.set("M " + key.toString() + " " + movieID.get(i) + " "
						+ (rating.get(i) - average) + " " + average);
				output.collect(null, valText);
			}

		} catch (Exception e) {
			// A way to debug run time exceptions when you dont have
			// access to cluster logs.
			String valueString = "";
			while (values.hasNext()) {
				valueString += values.next().toString();
			}

			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();

			Path pt = new Path("errorfile");
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
					fs.create(pt, true)));
			br.write(exceptionAsString + "\nkey: " + key.toString()
					+ "\nvalues: " + valueString);
			br.close();
		}

		// Output: (null, <M userid, movieid, normalizedrating>)
	}
}
