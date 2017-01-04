


import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class Partitionerexample extends Configured implements Tool
{
   //Map class
	
   public static class MapClass extends Mapper<LongWritable,Text,LongWritable,Text>
   {
      public void map(LongWritable key, Text value, Context context)
      {
         try{
        		String[] str1 = value.toString().split(",");
    	
        		context.write(new LongWritable(Long.parseLong(str1[1])), new Text(str1[2]+"," + str1[3]+"," + str1[4]));

    			//context.write(new Text(gender), new Text(value));
         }
         catch(Exception e)
         {
            System.out.println(e.getMessage());
         }
      }
   }
   
   //Reducer class
	
   public static class ReduceClass extends Reducer<LongWritable,Text,LongWritable,LongWritable>
   {
     
      public void reduce(LongWritable key, Iterable <Text> values, Context context) throws IOException, InterruptedException
      {
    	  String[] str2 = values.toString().split(",");
			long lol1 = (Long.parseLong(str2[0]) * Long.parseLong(str2[1]));
			context.write(key, new LongWritable(lol1));
			
       
      }
   }
   
   //Partitioner class
	
   public static class CaderPartitioner extends
   Partitioner < LongWritable, Text >
   {
      @Override
      public int getPartition(LongWritable key, Text value, int numReduceTasks)
      {
         String[] str = value.toString().split(",");
			int returnpar = -1;
			
			if (str[str.length-1].equals("MAH")) {
				returnpar = 0;
			} else if (str[str.length-1].equals("KAR")) {
				returnpar = 1;
			}
			return returnpar;


      }
   }
   
   @Override
   public int run(String[] arg) throws Exception
   {
      Configuration conf = getConf();
		
      Job job = Job.getInstance(conf, "topsal");
      job.setJarByClass(Partitionerexample.class);
		
      FileInputFormat.setInputPaths(job, new Path(arg[0]));
      FileOutputFormat.setOutputPath(job,new Path(arg[1]));
		
      job.setMapperClass(MapClass.class);
		
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      
      //set partitioner statement
		
      job.setPartitionerClass(CaderPartitioner.class);
      job.setReducerClass(ReduceClass.class);
      job.setNumReduceTasks(2);
      job.setInputFormatClass(TextInputFormat.class);
		
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
		
      System.exit(job.waitForCompletion(true)? 0 : 1);
      return 0;
   }
   
   public static void main(String ar[]) throws Exception
   {
      int res = ToolRunner.run(new Configuration(), new Partitionerexample(),ar);
      System.exit(0);
   }
}