import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PredictionDriver {
	public class PredictionMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void configure(JobConf conf) {
			// String var2String = conf.get("BIG_DATA");
			// BIG_DATA = Boolean.parseBoolean(var2String);
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: (lineNo, lineContent)

			// Split each line using seperator based on the dataset.
			String line[] = null;

			line = value.toString().split("\\s");

			// Output: (U userid, null)
			output.collect(new Text(line[0]), new Text("1"));
		}
	}

	public static void main(String[] args) throws Exception {
		// Job to compare give Predictions from the Product Matrix by adding
		// User Average and Movie Average to the Product Values to De-normalize
		// the Product Matrix output.

		// This file is written for small data only. Need to be updated for
		// large data.
		// This will not work for distributed machine.

		// Add back to product matrix from User and Movie average files.

		File inputFile = new File("/media/Ubuntu/movie_data/useravg");
		Scanner scanner = new Scanner(inputFile);

		HashMap<String, Float> userHs = new HashMap<String, Float>();
		while (scanner.hasNextLine()) {
			String lineStr = scanner.nextLine();
			String[] line = lineStr.split("\\s");
			userHs.put(line[0], Float.parseFloat(line[1]));
		}
		
		System.out.println("User Map: " + userHs.toString());

		inputFile = new File("/media/Ubuntu/movie_data/movieavg");
		scanner = new Scanner(inputFile);

		HashMap<String, Float> movieHs = new HashMap<String, Float>();
		while (scanner.hasNextLine()) {
			String lineStr = scanner.nextLine();
			String line[] = lineStr.split("\\s");
			movieHs.put(line[0], Float.parseFloat(line[1]));
		}
		
		System.out.println("Movie Map: " + movieHs.toString());

		System.out.println("Reading Product Matrix..");
		
		inputFile = new File("/media/Ubuntu/movie_data/productmatrix");
		scanner = new Scanner(inputFile);

		HashMap<String, Float> productHs = new HashMap<String, Float>();
		while (scanner.hasNextLine()) {
			String lineStr = scanner.nextLine();
			String line[] = lineStr.split(",");
			Float useravg = userHs.get(line[0]);
			Float movieavg = movieHs.get(line[1]);
			if (useravg == null || movieavg == null) {
				System.out.println("SWERR: These details not found.");
			} else {
				productHs.put(line[0] + "," + line[1],
						Float.parseFloat(line[2]) + useravg + movieavg);
			}
		}

		System.out.println("Reading Original Matrix.. ");
		
		inputFile = new File("/media/Ubuntu/movie_data/normalize");
		scanner = new Scanner(inputFile);

		HashMap<String, Float> originalHs = new HashMap<String, Float>();
		while (scanner.hasNextLine()) {
			String lineStr = scanner.nextLine();
			String line[] = lineStr.split("\\s");
			originalHs.put(line[1] + "," + line[2], Float.parseFloat(line[3]));
		}
		
		System.out.println("RMSE..");

		float sum = 0.0f;
		int count = 0;
		for (Map.Entry<String, Float> entry : originalHs.entrySet()) {
			Float a = entry.getValue();
			Float b = productHs.get(entry.getKey());
			if(b == null){
				System.out.println("No product value for: "+ entry.getKey());
				continue;
			}
			sum += Math.pow(a-b,2);
			count++;
		}

		System.out.println("RMSE: " + sum / count);

		System.out.println("Predicted ratings..");
		File outputFile = new File("/media/Ubuntu/movie_data/predictions");
		if (!outputFile.exists()) {
			outputFile.createNewFile();
		}

		BufferedWriter out;
		FileWriter fileStream = new FileWriter(outputFile, true);
		out = new BufferedWriter(fileStream);

		for (Map.Entry<String, Float> entry : productHs.entrySet()) {
			if (originalHs.get(entry.getKey()) == null) {
				out.write(entry.getKey() + "," + entry.getValue()+"\n");
			}
		}
		out.close();
	}
}
