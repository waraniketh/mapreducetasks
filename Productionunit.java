import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
public class Productionunit {
	public static class Mymapper extends Mapper<LongWritable,Text,Text,Text>{
		
		protected void map(LongWritable key,Text input,Context context) throws NumberFormatException, IOException, InterruptedException{
		String[] str=input.toString().split(",");	
			String outputkey=str[0];
			context.write(new Text(outputkey),new Text("p"+","+str[1]+","+str[2]));
		}
	}
	
public static class Mymapper1 extends Mapper<LongWritable,Text,Text,Text>{
		
		protected void map(LongWritable key,Text input,Context context) throws NumberFormatException, IOException, InterruptedException{
			String[] str=input.toString().split(",");	
			String outputkey=str[0];
			
			context.write(new Text(outputkey),new Text("s"+","+str[1]+","+str[2]));	
		}
	}
public static class Myreducer extends Reducer<Text,Text,Text,Text>{
	protected void reduce(Text key,Iterable<Text> input,Context context) throws IOException, InterruptedException{
		String str=input.toString();
			context.write(key,new Text(str));
	}
}	
	public static void main(String args[])throws IOException,ClassNotFoundException, InterruptedException{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(Productionunit.class);
		MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Mymapper.class);
		MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Mymapper1.class);
	    job.setReducerClass(Myreducer.class);
		job.setMapOutputKeyClass(Text.class);
		FileOutputFormat.setOutputPath(job,new Path(args[2]));
		job.waitForCompletion(true);
		
	}}
