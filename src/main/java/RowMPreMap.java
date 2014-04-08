import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class RowMPreMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text keyText = new Text();
		private Text valText = new Text();
		private static Boolean BIG_DATA;

		public void configure(JobConf conf) {
			String var2String = conf.get("BIG_DATA");
			BIG_DATA = Boolean.parseBoolean(var2String);
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: (lineNo, lineContent)

			// Split each line using seperator based on the dataset.
			String line[] = null;

			if (BIG_DATA)
				line = value.toString().split("::");
			else
				line = value.toString().split("\\s");

			keyText.set(line[0]);
			valText.set(line[1] + "," + line[2]);

			// Output: (userid, "movieid,rating")
			output.collect(keyText, valText);

		}
	}
