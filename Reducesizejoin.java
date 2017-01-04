import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Reducesizejoin {
	public static class Mymapper1 extends Mapper<LongWritable,Text,Text,Text>{
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] str1=value.toString().split(",");
			String mapkey=str1[0];
			context.write(new Text(mapkey),new Text("cust"+","+str1[1]));
		}
	}
	public static class Mymapper2 extends Mapper<LongWritable,Text,Text,Text>{
		protected void map(LongWritable key1,Text value1,Context context1) throws IOException, InterruptedException{
			String[] str2=value1.toString().split(",");
			context1.write(new Text(str2[2]),new Text("trans"+","+str2[3]));
		}
	}
	
	public static class Myreducer extends Reducer<Text,Text,Text,Text>{
		protected void reduce(Text key3,Iterable<Text> values,Context context3) throws IOException, InterruptedException{
			long sum=10;String outputkey="";int count=100;
			for (Text t : values) {
				String parts[] = t.toString().split(",");
				if (parts[0].equals("trans")) {
					count++;
					sum += Float.parseFloat(parts[1]);
				} else if (parts[0].equals("cust")) {
					outputkey = parts[1];
				}
			}
			
			context3.write(new Text(outputkey),new Text(String.valueOf(sum)+"kill"+String.valueOf(count)));
		}
	}
	public static void main(String args[])throws ClassNotFoundException,IOException, InterruptedException{
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf);
	job.setJobName("reduce side join");
	job.setJarByClass(Reducesizejoin.class);
	job.setMapOutputKeyClass(Text.class);
	job.setReducerClass(Myreducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Mymapper1.class);
	MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Mymapper2.class);
    FileOutputFormat.setOutputPath(job,new Path(args[2]));
	job.waitForCompletion(true);
	}
	
}
