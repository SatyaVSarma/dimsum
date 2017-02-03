import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 2017 - ENSAE - ELTDM
 * 
 * @author antoine isnardy
 * @author satya vengathesa sarma 
 * 
 * DIMSUM
 * Mapper: see LeanDIMSUMMapper algorithm in the paper
 * Reducer: Sums over all pairs.
 * 
 * Output: row, col, value
 *
 */
public class DimSum {
	public static class LeanDIMSUMMapper extends Mapper <Object, Text, Text, DoubleWritable> {
		
		private Text cols = new Text();
		private DoubleWritable contrib = new DoubleWritable();
		
		private final double threshold = 0.2;
		
		public void map (Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// Read norms
			HashMap<Integer, Double> norms = new HashMap<Integer, Double>();
			try {
				//Location of file in HDFS
	            Path pt = new Path("hdfs://localhost:9000//norms/part-r-00000");
	            FileSystem fs = FileSystem.get(new Configuration());
	            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
	            String line;
	            line = br.readLine();
	            while (line != null) {
	            	String[] tmp = line.split("\\s+");
	            	norms.put(new Integer(tmp[0]), new Double(tmp[1]));
	                line=br.readLine();
	            }
	        } catch(Exception e){}

			// Algorithm
			Double sqrt_gamma = Math.sqrt(4.0 * Math.log(norms.size()) / threshold);
			String[] row = value.toString().split(","); // row values
			Random rnd = new Random();
			// Loop through each pair of columns
			for (int j=0; j<row.length; j++) {
				Double r_j = Double.parseDouble(row[j]);
				// Why escape 0 values:
				// - Would bring nothing to b_jk
				// - Avoid potential division by 0 if the norm is 0
				if (r_j != 0) {
					Double p_j = Math.min(1.0, sqrt_gamma / norms.get(j));
					if (p_j >= rnd.nextDouble()) {
						for (int k=0; k<row.length; k++) {
							Double r_k = Double.parseDouble(row[k]);
							if (r_k != 0) {
								Double p_k = Math.min(1.0, sqrt_gamma / norms.get(k));
								if (p_k >= rnd.nextDouble()) {
									cols.set(j + "\t" + k);
									Double tmp = r_j * r_k;
									tmp /= (Math.min(sqrt_gamma, norms.get(j)) * Math.min(sqrt_gamma, norms.get(k)));
									contrib.set(tmp);
									context.write(cols, contrib);
								}
							}
						}
					}
				}
			}
		}
	}
	
	
	public static class DIMSUMReducer extends Reducer <Text, DoubleWritable, Text, DoubleWritable> {
		
		private DoubleWritable b_jk = new DoubleWritable();
		
		public void reduce (Text columns, Iterable<DoubleWritable> contribs, Context context)
				throws IOException, InterruptedException {
			Double sum_tmp = 0.0;
			for (DoubleWritable contrib: contribs) {
				sum_tmp += contrib.get();
			}
			b_jk.set(sum_tmp);
			context.write(columns, b_jk);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DIMSUM");
		job.setJarByClass(MatrixNorm.class);
		
		job.setMapperClass(LeanDIMSUMMapper.class);
		job.setCombinerClass(DIMSUMReducer.class);
		job.setReducerClass(DIMSUMReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}