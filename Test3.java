import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Test3 {
	
public static class MyMapper1 extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String[] str=value.toString().split(",");
		context.write(new Text(str[0]),new Text(str[1]+","+str[2]+","+str[3]));
	}
}	
public static class MyReducer extends Reducer<Text,Text,Text,LongWritable>{

	public void reduce(Text key,Iterable<Text> itr,Context context) throws IOException, InterruptedException{
		String[] test=key.toString().split("/t");
		for(Text itr1:itr){
		String[] str1=itr1.toString().split(",");
		long lol=Long.parseLong(str1[1])*Long.parseLong(str1[2]);
		context.write(new Text(test[1]),new LongWritable(lol));
		}		
	}
}

public static class MyPartitioner extends Partitioner{
public int getPartition(Object key,Object value,int numreducetasks){
	String[] spli=value.toString().split(",");int rep=-1;
	if(Integer.parseInt(spli[0])==101){
		rep=0;
	}
	if(Integer.parseInt(spli[0])==102){
		rep=1;
	}
	return rep;
}
}

public static void main(String args[])throws Exception{
Configuration conf=new Configuration();
Job job=Job.getInstance(conf);
job.setJarByClass(Test3.class);
job.setMapperClass(MyMapper1.class);
job.setReducerClass(MyReducer.class);
job.setPartitionerClass(MyPartitioner.class);
job.setNumReduceTasks(2);
job.setMapOutputKeyClass(Text.class);
FileInputFormat.addInputPath(job,new Path(args[0]));
FileOutputFormat.setOutputPath(job,new Path(args[1]));
job.waitForCompletion(true);

}

}
