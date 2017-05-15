package maharshi;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Continuationoffirst {

	public static class Mymapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		protected void map(LongWritable key, Text value, Context cont) throws IOException, InterruptedException {
			String war[] = value.toString().split("\t");
			cont.write(new Text(war[0]), new IntWritable(Integer.parseInt(war[1])));
		}
	}

	public static class Myreducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
		float total = 0;

		public void reduce(Text key1, Iterable<IntWritable> values, Context context)

		throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				if (key1.toString().equals("All")) {
					total = val.get();
				} else {
					count = val.get();
				}
			}
			
			float per = (count * 100) / total;
			context.write(key1, new FloatWritable(per));
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Continuationoffirst.class);
		//job.setNumReduceTasks(0);
		job.setMapperClass(Mymapper.class);
		job.setReducerClass(Myreducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

}