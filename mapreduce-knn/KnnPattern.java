import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KnnPattern
{
	
	// WritableComparable class for a paired Double and String (distance and model)
	// This is a custom class for MapReduce to pass a double and a String through context
	// as one serializable object.
	// This example only implements the minimum required methods to make this job run. To be
	// deployed robustly is should include ToString(), hashCode(), WritableComparable interface
	// if this object was intended to be used as a key etc.
		public static class DoubleString implements WritableComparable<DoubleString>
		{
			private Double distance = 0.0;
			private String model = null;

			public void set(Double lhs, String rhs)
			{
				distance = lhs;
				model = rhs;
			}
			
			public Double getDistance()
			{
				return distance;
			}
			
			public String getModel()
			{
				return model;
			}
			
			@Override
			public void readFields(DataInput in) throws IOException
			{
				distance = in.readDouble();
				model = in.readUTF();
			}
			
			@Override
			public void write(DataOutput out) throws IOException
			{
				out.writeDouble(distance);
				out.writeUTF(model);
			}
			
			@Override
			public int compareTo(DoubleString o)
			{
				return (this.model).compareTo(o.model);
			}
		}
	
	// The mapper class accepts an object and text (row identifier and row contents) and outputs
	// two MapReduce Writable classes, NullWritable and DoubleString (defined earlier)
	public static class KnnMapper extends Mapper<Object, Text, NullWritable, DoubleString>
	{
		DoubleString distanceAndModel = new DoubleString();
		TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();
		
		// Declaring some variables which will be used throughout the mapper
		int K;
	    
		// 6 features for the test point (S)
		double[] sFeatures = new double[6];
		
		// Takes a double and returns its squared value.
		private double squaredDistance(double n1)
		{
			return Math.pow(n1,2);
		}

		// Calculate Euclidean distance squared between two points with 6 features
		private double totalSquaredDistance(double[] R, double[] S)
		{	
			double sum = 0.0;
			for (int i = 0; i < 6; i++) {
				sum += squaredDistance(S[i] - R[i]);
			}
			return sum;
		}

		// The @Override annotation causes the compiler to check if a method is actually being overridden
		// (a warning would be produced in case of a typo or incorrectly matched parameters)
		@Override
		// The setup() method is run once at the start of the mapper and is supplied with MapReduce's
		// context object
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			// Read K and features from Configuration
			K = conf.getInt("knn.k", 3);
			for (int i = 0; i < 6; i++) {
				sFeatures[i] = conf.getDouble("knn.feature." + i, 0.0);
			}
		}
				
		@Override
		// The map() method is run by MapReduce once for each row supplied as the input data
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			// Skip header line
			String rLine = value.toString();
			if (rLine.startsWith("feature_0") || rLine.trim().isEmpty()) {
				return;
			}
			
			// Tokenize the input line from the csv file
			StringTokenizer st = new StringTokenizer(rLine, ",");
			
			// Read 6 features
			double[] rFeatures = new double[6];
			for (int i = 0; i < 6; i++) {
				rFeatures[i] = Double.parseDouble(st.nextToken().trim());
			}
			// Read target (the class label)
			String rTarget = st.nextToken().trim();
			
			// Calculate distance between this training point and the test point
			double tDist = totalSquaredDistance(rFeatures, sFeatures);
			
			// Add the total distance and corresponding target for this row into the TreeMap
			KnnMap.put(tDist, rTarget);
			// Only K distances are required, so if the TreeMap contains over K entries, remove the last one
			if (KnnMap.size() > K)
			{
				KnnMap.remove(KnnMap.lastKey());
			}
		}

		@Override
		// The cleanup() method is run once after map() has run for every row
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			// Loop through the K key:values in the TreeMap
			for(Map.Entry<Double, String> entry : KnnMap.entrySet())
			{
				  Double knnDist = entry.getKey();
				  String knnModel = entry.getValue();
				  // distanceAndModel is the instance of DoubleString declared aerlier
				  distanceAndModel.set(knnDist, knnModel);
				  // Write to context a NullWritable as key and distanceAndModel as value
				  context.write(NullWritable.get(), distanceAndModel);
			}
		}
	}

	// The reducer class accepts the NullWritable and DoubleString objects just supplied to context and
	// outputs a NullWritable and a Text object for the final classification.
	public static class KnnReducer extends Reducer<NullWritable, DoubleString, NullWritable, Text>
	{
		TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();
		int K;
		
		@Override
		// setup() again is run before the main reduce() method
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			K = conf.getInt("knn.k", 3);
		}
		
		@Override
		// The reduce() method accepts the objects the mapper wrote to context: a NullWritable and a DoubleString
		public void reduce(NullWritable key, Iterable<DoubleString> values, Context context) throws IOException, InterruptedException
		{
			// values are the K DoubleString objects which the mapper wrote to context
			// Loop through these
			for (DoubleString val : values)
			{
				String rModel = val.getModel();
				double tDist = val.getDistance();
				
				// Populate another TreeMap with the distance and model information extracted from the
				// DoubleString objects and trim it to size K as before.
				KnnMap.put(tDist, rModel);
				if (KnnMap.size() > K)
				{
					KnnMap.remove(KnnMap.lastKey());
				}
			}	

				// This section determines which of the K values (models) in the TreeMap occurs most frequently
				// by means of constructing an intermediate ArrayList and HashMap.

				// A List of all the values in the TreeMap.
				List<String> knnList = new ArrayList<String>(KnnMap.values());

				Map<String, Integer> freqMap = new HashMap<String, Integer>();
			    
			    // Add the members of the list to the HashMap as keys and the number of times each occurs
			    // (frequency) as values
			    for(int i=0; i< knnList.size(); i++)
			    {  
			        Integer frequency = freqMap.get(knnList.get(i));
			        if(frequency == null)
			        {
			            freqMap.put(knnList.get(i), 1);
			        } else
			        {
			            freqMap.put(knnList.get(i), frequency+1);
			        }
			    }
			    
			    // Examine the HashMap to determine which key (model) has the highest value (frequency)
			    String mostCommonModel = null;
			    int maxFrequency = -1;
			    for(Map.Entry<String, Integer> entry: freqMap.entrySet())
			    {
			        if(entry.getValue() > maxFrequency)
			        {
			            mostCommonModel = entry.getKey();
			            maxFrequency = entry.getValue();
			        }
			    }
			    
			// Finally write to context another NullWritable as key and the most common model just counted as value.
			context.write(NullWritable.get(), new Text(mostCommonModel)); // Use this line to produce a single classification
//			context.write(NullWritable.get(), new Text(KnnMap.toString()));	// Use this line to see all K nearest neighbours and distances
		}
	}

	// Main program to run: By calling MapReduce's 'job' API it configures and submits the MapReduce job.
	public static void main(String[] args) throws Exception
	{
		// Create configuration
		Configuration conf = new Configuration();
		
		if (args.length < 3 || args.length > 4)
		{
			System.err.println("Usage: KnnPattern <in> <out> <parameter file> [num_reducers]");
			System.exit(2);
		}

		// Read parameter file and set in Configuration (supports HDFS or local)
		String knnParams;
		Path paramPath = new Path(args[2]);
		FileSystem fs = paramPath.getFileSystem(conf);
		try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(paramPath)))) {
			knnParams = br.readLine();
		}
		StringTokenizer st = new StringTokenizer(knnParams, ",");
		int K = Integer.parseInt(st.nextToken().trim());
		conf.setInt("knn.k", K);
		for (int i = 0; i < 6; i++) {
			conf.setDouble("knn.feature." + i, Double.parseDouble(st.nextToken().trim()));
		}

		// Get number of reducers (default 1)
		int numReducers = 1;
		if (args.length == 4) {
			numReducers = Integer.parseInt(args[3]);
		}

		// Create job
		Job job = Job.getInstance(conf, "Find K-Nearest Neighbour");
		job.setJarByClass(KnnPattern.class);
		
		// Setup MapReduce job
		job.setMapperClass(KnnMapper.class);
		job.setReducerClass(KnnReducer.class);
		job.setNumReduceTasks(numReducers);

		// Specify key / value
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(DoubleString.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
				
		// Input (the data file) and Output (the resulting classification)
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Execute job and return status
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}