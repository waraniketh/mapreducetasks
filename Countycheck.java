import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class Countycheck {

	public static class Mymapper extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] str=value.toString().split(",");
			context.write(new Text(str[1]),new Text(str[3]+","+str[5]));
			
		}
	}
	public static class Myreducer extends Reducer<Text,Text,Text,Text>{
		
		public void reduce(Text key,Iterable<Text> itr,Context context) throws IOException, InterruptedException{
            long sum=0;
			for(Text tet:itr){
				String[] check=tet.toString().split(",");
				if(check[0].equals("county")){
					sum=sum+Long.parseLong(check[1]);
				}
				else{
					
					sum=100;
				}
			}context.write(key,new Text(String.valueOf(sum)));
			
		}
	}

public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf,"county");
		job.setJarByClass(Countycheck.class);
		job.setMapperClass(Mymapper.class);
        job.setReducerClass(Myreducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);	
	}

}
