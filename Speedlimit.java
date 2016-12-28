
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Speedlimit {
	public static class Maptask3 extends Mapper<LongWritable,Text,Text,LongWritable>{
	public void map(LongWritable key,Text values,Context context)throws IOException,InterruptedException{
	String[] str=values.toString().split(",");
	LongWritable lo=new LongWritable(Integer.parseInt(str[1]));
	context.write(new Text(str[0]),lo);
	}	}
 public static class Reducetask3 extends Reducer<Text,LongWritable,Text,LongWritable>{
	 public void reduce(Text key,Iterable<LongWritable> itr,Context context)throws IOException,InterruptedException{
	 long count=0,count1=0;long per=0;
	 for(LongWritable goog:itr)
	 {if(goog.get()>65){
		 count1++;}	 
	    count++;} 
	 per=(count1*100/count);
	 context.write(key,new LongWritable(per));
	 }
 }	
 public static void main(String[] args)throws Exception{
	 Configuration conf=new Configuration();
	 Job job=Job.getInstance(conf,"Speedlimit1");
	 job.setJarByClass(Speedlimit.class);
	 job.setMapperClass(Maptask3.class);
	 job.setReducerClass(Reducetask3.class);
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(LongWritable.class);
	 FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 System.exit(job.waitForCompletion(true) ? 0 : 1);
	 }	
	 }
	

