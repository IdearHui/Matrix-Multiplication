package onestep;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CaculateMatrix {
	
	public static class CMMap extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String lines = value.toString();
        	String[] line = lines.split(" ");
        	int i = context.getConfiguration().getInt("i", 0);
        	int k = context.getConfiguration().getInt("k", 0);
        	if(line[3].equals("A"))
	        	for(int K_index=1;K_index<k+1;K_index++)
	        		context.write(new Text(line[0]+","+K_index), new Text(line[3]+"\t"+line[1]+"\t"+line[2]));
        	else
	        	for(int I_index=1;I_index<i+1;I_index++)
	        		context.write(new Text(I_index+","+line[1]), new Text(line[3]+"\t"+line[0]+"\t"+line[2]));
		}
	}
	public static class CMReduce extends Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			Iterator value = values.iterator();
	        List<String> arrayA = new ArrayList<String>();
	        List<String> arrayB = new ArrayList<String>();
	        int v = 0;
	        while(value.hasNext()){
	        	String record = value.next().toString();
	        	String[] r = record.split("\t");
	        	v = Integer.parseInt(r[2]);
	        	if(r[0].equals("A"))
	        		arrayA.add(Integer.parseInt(r[1])+"\t"+v);
	        	if(r[0].equals("B"))
	        		arrayB.add(Integer.parseInt(r[1])+"\t"+v);
	        }
	        int redvalue = 0;
	        for(int m=0;m<arrayA.size();m++){
	        	String[] arrA = arrayA.get(m).split("\t");
	        	int A_j = Integer.parseInt(arrA[0]);
	        	int i_v = Integer.parseInt(arrA[1]);
	        	for(int n=0;n<arrayB.size();n++){		        	
		        	String[] arrB = arrayB.get(n).split("\t");		        	
		        	int B_j = Integer.parseInt(arrB[0]);
		        	int k_v = Integer.parseInt(arrB[1]);
		        	
					if(A_j == B_j)
		        		redvalue += i_v * k_v;
	        	}
	        }
	        context.write(key, new IntWritable(redvalue));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.setInt("i", Integer.parseInt(args[2]));
		conf.setInt("j", Integer.parseInt(args[3]));
		conf.setInt("k", Integer.parseInt(args[4]));
		Job job = new Job(conf, "CaculateMatrix");
		job.setJarByClass(CaculateMatrix.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(CMMap.class);
		job.setReducerClass(CMReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
