import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
public class VerifyReducejoin {
public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String[] str=value.toString().split(";");
		context.write(new Text(str[1]),new Text("p1"+","+str[4]));
	}	
}
public static class MyMapper1 extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String[] str=value.toString().split("\t");
		context.write(new Text(str[0]),new Text("p2"+","+str[0]));
	}
}
	public static class Myreducer extends Reducer<Text,Text,Text,LongWritable>{
		public void reduce(Text key1,Iterable<Text> value2,Context context) throws IOException, InterruptedException{
			for(Text value1:value2){
			String[] str1=value1.toString().split(",");
			if(str1[0].equals("p1")){
			long totalearners=Long.parseLong(str1[1]);
			context.write(key1,new LongWritable(totalearners));
			}else if(str1[0].contains("p2")){
			context.write(new Text("gotit"),new LongWritable(Long.parseLong("10")));
			//context.write(key1,new LongWritable(Long.parseLong(str1[1])));
			}
			
			}
			}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf,"myfirstpro");
		job.setJarByClass(VerifyReducejoin.class);
        job.setReducerClass(Myreducer.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MyMapper.class); 
		MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,MyMapper1.class); 
		job.setMapOutputKeyClass(Text.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
		job.waitForCompletion(true);	
	}
	
	
}
