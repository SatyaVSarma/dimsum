import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
 * Matrix norm
 * Mapper: for each row r, emit <column_j, r_ij**2>
 * Combiner: for each column_j, emit <column_j, sum_i(r_ij**2)>
 * Reducer: for each column_j, emit <column_j, sqrt(sum_i(r_ij**2)>
 * 
 * Outputs in a single file to make life easier next
 *
 */
public class MatrixNorm {
	public static class NormMapper extends Mapper <Object, Text, IntWritable, DoubleWritable> {
		
		private IntWritable column = new IntWritable();
		private DoubleWritable product = new DoubleWritable();
		
		public void map (Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] row = value.toString().split(","); // row values
			
			for (int i=0; i<row.length; i++) {
				column.set(i);
				Double prod = Math.pow(Double.parseDouble(row[i]), 2);
				product.set(prod);
				context.write(column, product);
			}
		}
	}
	
	public static class NormCombiner extends Reducer <IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		
		private DoubleWritable sum_of_squares = new DoubleWritable();
		
		public void reduce (IntWritable column, Iterable<DoubleWritable> products, Context context)
				throws IOException, InterruptedException {
			Double sum_tmp = 0.0;
			for (DoubleWritable prod: products) {
				sum_tmp += prod.get();
			}
			sum_of_squares.set(sum_tmp);
			context.write(column, sum_of_squares);
		}
	}
	
	public static class NormReducer extends Reducer <IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		
		private DoubleWritable norm = new DoubleWritable();
		
		public void reduce (IntWritable column, Iterable<DoubleWritable> products, Context context)
				throws IOException, InterruptedException {
			Double sum_tmp = 0.0;
			for (DoubleWritable prod: products) {
				sum_tmp += prod.get();
			}
			sum_tmp = Math.sqrt(sum_tmp);
			norm.set(sum_tmp);
			context.write(column, norm);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Norm computation");
		job.setJarByClass(MatrixNorm.class);
		
		job.setMapperClass(NormMapper.class);
		job.setCombinerClass(NormCombiner.class);
		job.setReducerClass(NormReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}