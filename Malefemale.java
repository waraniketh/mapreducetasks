import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
public class Malefemale {
public static class Mymapper extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String[] str=value.toString().split(",");
		context.write(new Text(str[1]),new Text(str[5]+","+str[6]));
	}
	
	
}
	public static class Myreducer extends Reducer<Text,Text,Text,LongWritable>{
	
		public void reduce(Text key1,Iterable<Text> itr,Context context) throws IOException, InterruptedException{
			long total=0;
			for(Text male:itr){
			String[] str1=male.toString().split(",");
			long male1=Long.parseLong(str1[0]);
			long female=Long.parseLong(str1[1]);
			total=total+male1+female;
			}context.write(key1,new LongWritable(total));}}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf,"myfirstpro");
		job.setJarByClass(Malefemale.class);
		job.setMapperClass(Mymapper.class);
         job.setReducerClass(Myreducer.class);
		job.setMapOutputKeyClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);	
	}
	
	
}

