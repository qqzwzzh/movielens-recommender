import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ColMPreMap extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valText = new Text();

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		// Input: (lineNo, lineContent)
		// Split each line using seperator based on the dataset.

		String line[] = null;

		line = value.toString().split("\\s");

		keyText.set(line[2]);
		valText.set(line[1] + "," + line[3] + "," + line[4]);

		// Output: (userid, "movieid,rating")
		output.collect(keyText, valText);
	}
}