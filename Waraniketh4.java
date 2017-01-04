import  java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.LongWritable;
public class Waraniketh4 {
public static class Mymapper1 extends Mapper<LongWritable,Text,LongWritable,Text>{
	
protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
	
	LongWritable outputkey=new LongWritable();
	Text outputvalue=new Text();
	String[] val=value.toString().split(",");
	outputkey.set(Long.parseLong(val[1]));
	outputvalue.set(val[4]+","+val[2]+","+val[3]+",");
	context.write(outputkey,outputvalue);
}	
}
public static class Myreducer extends Reducer<LongWritable,Text,LongWritable,LongWritable>{
protected void reduce(LongWritable key,Iterable<Text> itr,Context context)throws IOException,InterruptedException{
   
	for(Text lol:itr){
		String[] val=lol.toString().split(",");
    long lol1=Long.parseLong(val[2])*Long.parseLong(val[1]);
	context.write(key,new LongWritable(lol1));}
	
}	
	
}
public static class MyPartitioner extends Partitioner<LongWritable,Text>{
	
public int getPartition(LongWritable key,Text value,int numreducetask){

	int returnval=-1;
	String[] str2=value.toString().split(",");
	if(str2[0].equals("MAH")){
	returnval=0;	
	}
	else if(str2[0].equals("KAR")){
		returnval=1;
	}
	return returnval;
}	
}

public static void main(String args[]) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException	
{
Configuration conf = new Configuration();
	
Job job = Job.getInstance(conf, "topsal");
job.setJarByClass(Waraniketh4.class);
	
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
job.setMapperClass(Mymapper1.class);
	
job.setMapOutputKeyClass(LongWritable.class);
job.setMapOutputValueClass(Text.class);

//set partitioner statement
	
job.setPartitionerClass(MyPartitioner.class);
job.setReducerClass(Myreducer.class);
job.setNumReduceTasks(2);
//job.setOutputFormatClass(TextInputFormat.class);
job.setOutputKeyClass(LongWritable.class);
job.setOutputValueClass(LongWritable.class);
	
System.exit(job.waitForCompletion(true)? 0 : 1);
	
}	
}
