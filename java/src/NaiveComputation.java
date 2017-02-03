import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
 * Naive implementation of DimSum:
 * End goal is to output dot products between columns of A (general term a_ij)
 * 
 * Mapper: for each row r, emit <(column_i, column_j), a_ri * a_rj>
 * Reducer: for each pair of columns, emit <(column_i, column_j), sum(a_ri * a_rj)>
 * Reducer output exactly stands for dot products between columns of A
 *
 */
public class NaiveComputation {
	public static class NaiveMapper extends Mapper <Object, Text, Text, DoubleWritable> {
		
		private Text pairOfColumns = new Text();
		private DoubleWritable product = new DoubleWritable();
		
		public void map (Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] row = value.toString().split(","); // row values
			
			// Loop through each pair of columns
			for (int i=0; i<row.length; i++) {
				for (int j=0; j<row.length; j++) {
					pairOfColumns.set(i + "," + j);
					Double prod = Double.parseDouble(row[i]) * Double.parseDouble(row[j]);
					product.set(prod);
					context.write(pairOfColumns, product); // Emit products
				}
			}
		}
	}
	
	public static class NaiveReducer extends Reducer <Text, DoubleWritable, Text, DoubleWritable> {
		
		private DoubleWritable sum = new DoubleWritable();
		
		public void reduce (Text pairOfColumns, Iterable<DoubleWritable> products, Context context)
				throws IOException, InterruptedException {
			Double sum_tmp = 0.0;
			for (DoubleWritable prod: products) {
				sum_tmp += prod.get();
			}
			sum.set(sum_tmp);
			context.write(pairOfColumns, sum);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Naive DimSum");
		job.setJarByClass(NaiveComputation.class);
		
		job.setMapperClass(NaiveMapper.class);
		job.setCombinerClass(NaiveReducer.class); // Legal since operations are the same 
		job.setReducerClass(NaiveReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
