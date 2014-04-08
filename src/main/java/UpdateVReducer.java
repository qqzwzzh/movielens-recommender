import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class UpdateVReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	HashMap<Integer, Float[]> uFeatureHm = new HashMap<Integer, Float[]>();
	private static int noOfCommonFeatures;

	public void configure(JobConf job) {
		System.out.println("Configuring UpdateUReducer");

		String commonFeaturesStr = job.get("noOfCommonFeatures");
		noOfCommonFeatures = Integer.parseInt(commonFeaturesStr);

		String uPath = job.get("uPath");
		System.out.println("UPATH: " + uPath);
		loadUMatrix(new Path(uPath));

	}

	private void loadUMatrix(Path cachePath) {
		System.out.println("Loading V Matrix..");

		try {
			FileSystem fs = FileSystem.get(new Configuration());

			FileStatus[] files = fs.listStatus(cachePath);
			for (FileStatus f : files) {
				// If that is a temp file, ingore it.
				if (new File(f.getPath().toString()).getName().startsWith("_"))
					continue;
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(f.getPath())));
				String line;

				// Input: (U UserID featureIndex featureValue)
				while ((line = br.readLine()) != null) {
					String[] tokens = line.split("\\s");
					int userid = Integer.parseInt(tokens[1]);
					String featureIndex = tokens[2];
					int fi = Integer.parseInt(featureIndex);
					String featureValue = tokens[3];
					float fv = Float.parseFloat(featureValue);

					// Storing each feature value against movieID and
					// featureIndex.
					// movieID starts from 1 and featureIndex starts from 0.
					// So we store at fi - 1.
					Float[] featureValues = uFeatureHm.get(userid);
					if (featureValues == null) {
						featureValues = new Float[noOfCommonFeatures];
						featureValues[fi - 1] = fv;
						uFeatureHm.put(userid, featureValues);
					} else {
						featureValues[fi - 1] = fv;
					}
				}

				br.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("SWERR: File error.");
		}
	}

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		// Input: (movieID, <list of M/V entries with that movieID in format
		// M/V userid/featureid movieid value>)

		ArrayList<Integer> userIDs = new ArrayList<Integer>();
		ArrayList<Float> ratings = new ArrayList<Float>();
		ArrayList<Float> productUV = new ArrayList<Float>();

		float[] vFeature = new float[10];

		while (values.hasNext()) {
			String line[] = values.next().toString().split("\\s");

			if (line[0].equals("M")) {

				// Store all Movie IDs and ratings into lists. We will use
				// them later for computation based on the index.

				userIDs.add(Integer.parseInt(line[1]));
				ratings.add(Float.parseFloat(line[3]));

				// Initialize the productUV to 0.
				productUV.add(0f);

			} else if (line[0].equals("V")) {

				// Store the feature values against their index.
				// Since the U matrix store FI starting with 1, do -1 here.
				vFeature[Integer.parseInt(line[1]) - 1] = Float
						.parseFloat(line[3]);

			} else {
				System.out.println("SWERR: Invalid record.");
			}
		}

		// Compute the new feature values for the current movie row in V.

		// Update the productUV
		// For each movie vector, we are multiplying with corresponding
		// vector in U matrix which is the user vector
		for (int i = 0; i < userIDs.size(); i++) {
			int userID = userIDs.get(i);
			float sum = 0;
			for (int j = 0; j < noOfCommonFeatures; j++) {
				sum += vFeature[j] * uFeatureHm.get(userID)[j];
			}
			productUV.set(i, sum);
		}

		// Update the feature values

		// Pick a random permutation of feature vector to start the update.
		ArrayList<Integer> featureIndex = new ArrayList<Integer>(10);
		for (int i = 0; i < noOfCommonFeatures; i++)
			featureIndex.add(i);
		Collections.shuffle(featureIndex);

		// Start to process features as per the permutation order.
		for (int featureX : featureIndex) {

			// The actual featureIndex we are processing now.
			int i = featureIndex.get(featureX);

			// For each userID

			float innerProduct = 0;
			float ujSquare = 0;
			for (int j = 0; j < userIDs.size(); j++) {
				int userID = userIDs.get(j);
				float subtract = ratings.get(j) - productUV.get(j)
						+ vFeature[i] * uFeatureHm.get(userID)[i];
				innerProduct += subtract * uFeatureHm.get(userID)[i];
				ujSquare += uFeatureHm.get(userID)[i]
						* uFeatureHm.get(userID)[i];
			}

			float updatedFeatureValue = (float) innerProduct / ujSquare;

			for (int j = 0; j < userIDs.size(); j++) {
				int userID = userIDs.get(j);

				// Subtract old feature contribution and add new
				// feature contribution.
				productUV.set(j, productUV.get(j) + uFeatureHm.get(userID)[i]
						* (updatedFeatureValue - vFeature[i]));
			}

			vFeature[i] = updatedFeatureValue;
		}

		// Output: (U userId featureIndex featureValue)

		for (int i = 0; i < noOfCommonFeatures; i++) {
			output.collect(null, new Text("V " + (i + 1) + " " + key.toString()
					+ " " + vFeature[i]));
		}

	}

}
