
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
public class Collegeearnings {
public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String[] str=value.toString().split(";");
		context.write(new Text("aniketh"),new Text(str[23]));
	}
	
	
}
	public static class Myreducer extends Reducer<Text,Text,Text,LongWritable>{
	
		public void reduce(Text key1,Iterable<Text> value2,Context context) throws IOException, InterruptedException{
			long sum=0;
			for(Text value1:value2){
			String[] str1=value1.toString().split(",");
			long male=Long.parseLong(str1[0]);
			sum=sum+male;
			}
			context.write(key1,new LongWritable(sum));
			}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf,"myfirstpro");
		job.setJarByClass(Collegeearnings.class);
		job.setMapperClass(MyMapper.class);
         job.setReducerClass(Myreducer.class);
		job.setMapOutputKeyClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);	
	}
	
	
}
