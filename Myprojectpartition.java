import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
public class Myprojectpartition {
public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String[] str=value.toString().split(";");
		context.write(new Text(str[1]),new Text(str[2]+","+str[4]));
	}
	
	
}
	public static class Myreducer extends Reducer<Text,Text,Text,LongWritable>{
	
		public void reduce(Text key1,Iterable<Text> value2,Context contexe) throws IOException, InterruptedException{
			for(Text value1:value2){
			String[] str1=value1.toString().split(",");
				contexe.write(new Text(str1[0]),new LongWritable(Long.parseLong(str1[1])));
				}
		}}
		public static class Mypartitioner extends Partitioner<Text,Text>{
			
			public int getPartition(Text key,Text value,int numreducetasks){
				
				if(value.toString().contains("nation")){
					return 0;
				}else if(value.toString().contains("region")){
					return 1;
				}
				else if(value.toString().contains("division")){
					return 3;
				}
				else{
				return 2;}
			}
		}
		
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf,"myfirstpro");
		job.setJarByClass(Myprojectpartition.class);
		job.setMapperClass(MyMapper.class);
         job.setReducerClass(Myreducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setNumReduceTasks(4);
		job.setPartitionerClass(Mypartitioner.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);	
	}
	
	
}
