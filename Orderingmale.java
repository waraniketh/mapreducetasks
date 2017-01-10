import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
public class Orderingmale {
public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String[] str=value.toString().split("\t");
		context.write(new Text("waraniketh"),new Text(str[1]+","+str[0]));
	}
}
public static class Myreducer extends Reducer<Text,Text,Text,LongWritable>{
	     long[] order=new long[100];int i=0;
		public void reduce(Text key1,Iterable<Text> value2,Context context) throws IOException, InterruptedException{
			for(Text value1:value2){
			String[] str1=value1.toString().split(",");
			long maleorder=Long.parseLong(str1[0]);
			order[i]=maleorder;
            i++;
			}
			
		Arrays.sort(order);
		for(int j=order.length-1;j>0;j--){
			if(order[j]>0){
		context.write(new Text(String.valueOf(j)),new LongWritable(order[j]));	
			}}
		}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf,"myfirstpro");
		job.setJarByClass(Orderingmale.class);
		job.setMapperClass(MyMapper.class);
	    job.setReducerClass(Myreducer.class);
		job.setMapOutputKeyClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);	
	}
	
	
}
