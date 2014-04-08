import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ColMPreReduce extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	private Text valText = new Text();

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		// Input: (movieid, List<userid, rating, prev-average>)

		float sum = 0.0F;
		int totalRatingCount = 0;

		ArrayList<String> userID = new ArrayList<String>();
		ArrayList<Float> rating = new ArrayList<Float>();
		ArrayList<Float> prevAverage = new ArrayList<Float>();

		while (values.hasNext()) {
			String[] userRatingPair = values.next().toString().split(",");
			userID.add(userRatingPair[0]);
			Float parseRating = Float.parseFloat(userRatingPair[1]);
			rating.add(parseRating);
			Float parsePrevAverage = Float.parseFloat(userRatingPair[2]);
			prevAverage.add(parsePrevAverage);

			sum += parseRating;
			totalRatingCount++;
		}

		Float average = ((float) sum) / totalRatingCount;

		for (int i = 0; i < userID.size(); i++) {
			valText.set("M " + userID.get(i) + " " + key.toString() + " "
					+ (rating.get(i) - average) + " "
					+ (average + prevAverage.get(i)));
			output.collect(null, valText);
		}

		// total average subtracted will be added to product matrix when making
		// prediction.
		// Output1: (null, <M userid, movieid, normalizedrating, totalaverage>)

	}
}
