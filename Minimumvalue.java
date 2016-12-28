
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Minimumvalue {
	public static class Maptask1 extends Mapper<LongWritable,Text,Text,LongWritable>{
	public void map(LongWritable key,Text values,Context context)throws IOException,InterruptedException{
	String[] str=values.toString().split(",");
    LongWritable lo=new LongWritable(Long.parseLong(str[7]));
	context.write(new Text(str[1]),lo);		
	}	}
 public static class Reducetask1 extends Reducer<Text,LongWritable,Text,LongWritable>{
	 public void reduce(Text key,Iterable<LongWritable> itr,Context context)throws IOException,InterruptedException{
    long min=999999999;
	for(LongWritable goog:itr)
	 {
		if(min>goog.get()){
		min=goog.get();	
		} 
	 }
	 context.write(key,new LongWritable(min));
	 }
 }	
 public static void main(String[] args)throws Exception{
	 Configuration conf=new Configuration();
	 Job job=Job.getInstance(conf,"Stockminprice");
	 job.setJarByClass(Minimumvalue.class);
	 job.setMapperClass(Maptask1.class);
	 //job.setCombinerClass(IntSumReducer.class);
	 job.setReducerClass(Reducetask1.class);
	 job.setOutputKeyClass(Text.class);//job.setInputFormatClass(Text.class);
	 job.setOutputValueClass(LongWritable.class);
	 FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 System.exit(job.waitForCompletion(true) ? 0 : 1);
	 }	
	 }
	

