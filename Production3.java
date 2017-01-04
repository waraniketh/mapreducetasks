import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;

public class Production3 {
	public static class Productionmap extends Mapper<LongWritable,Text,Text,Text>{
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String str=value.toString();
		String[] str1=str.split(",");
		context.write(new Text(str1[0]),new Text(str1[1]+","+str1[1]));
	}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(Production3.class);
		job.setMapperClass(Productionmap.class);
		job.setMapOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job,new  Path(args[0]));	
	    FileOutputFormat.setOutputPath(job,new Path( args[1]));
	    job.waitForCompletion(true);
	}
}
