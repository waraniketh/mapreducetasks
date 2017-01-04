import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class productionunit1 {
public static class Mymapper1 extends Mapper<LongWritable,Text,LongWritable,Text>{
		
		protected void map(LongWritable key,Text input,Context context) throws NumberFormatException, IOException, InterruptedException{
			String[] str=input.toString().split(",");	
			String outputkey=str[0];
			
			context.write(new LongWritable(Long.parseLong(str[0])),new Text("s"+","+str[1]));	
		}
	}
	public static void main(String args[])throws IOException,ClassNotFoundException, InterruptedException{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(productionunit1.class);
		//MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Mymapper.class);
		//MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Mymapper1.class);
		//job.setReducerClass(Myreducer.class);
		//job.setMapOutputKeyClass(Text.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
		
	}
	
	

}
