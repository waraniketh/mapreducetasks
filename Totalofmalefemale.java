import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Totalofmalefemale {

public static class Mymapper extends Mapper<LongWritable,Text,NullWritable,LongWritable>{
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		
		String[] str=value.toString().split(",");
		context.write(NullWritable.get(),new LongWritable(Long.parseLong(str[4])));
	}
}
public static class Myreducer extends Reducer<NullWritable,LongWritable,NullWritable,LongWritable>{
	
	public void reduce(NullWritable key,Iterable<LongWritable> itr,Context context) throws IOException, InterruptedException{
		long total=0;
		for(LongWritable obj:itr){
			
			total=total+obj.get();
		
		}
		context.write(NullWritable.get(),new LongWritable(total));
	
	}
}
public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf,"maletotal");
		job.setJarByClass(Totalofmalefemale.class);
		job.setMapperClass(Mymapper.class);
        job.setReducerClass(Myreducer.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);	
	}
	
}
