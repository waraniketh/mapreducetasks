package maharshi;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Firstquery {

	public static class Mymapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		protected void map(LongWritable key, Text value, Context cont) throws IOException, InterruptedException {
			String war[] = value.toString().split("@");
			if (war.length > 2) {
				cont.write(new Text(war[1]), new IntWritable(1));
				cont.write(new Text("All"), new IntWritable(1));

			}
		}
	}

	public static class Myreducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key1, Iterable<IntWritable> values, Context context)

		throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count++;
			}
			context.write(key1, new IntWritable(count));
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Firstquery.class);
		job.setMapperClass(Mymapper.class);
		job.setReducerClass(Myreducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

}