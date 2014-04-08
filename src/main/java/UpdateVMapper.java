import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class UpdateVMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		// Input: (M/V userid/featureid movieid rating)
		String[] line = value.toString().split("\\s");

		// Output: (movieid, <value>)
		output.collect(new Text(line[2]), value);
	}

}
