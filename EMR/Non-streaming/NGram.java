import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGram {

    public static class NGramMap extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            
            String line = value.toString();  
            if (line.length() == 0)  // for empty line
                return;
            
            // tokenize
            line = line.trim().toLowerCase().replaceAll("[^a-zA-Z]+", " ");
            StringTokenizer tokenizer = new StringTokenizer(line);
            
            ArrayList<String> list = new ArrayList<String>();
            while (tokenizer.hasMoreTokens()) {
                list.add(tokenizer.nextToken());
            }
            
            // map
            for (int i = 0; i < list.size(); i++) {
                StringBuilder sb = new StringBuilder(list.get(i));
                word.set(sb.toString());
                context.write(word, one);
                
                for (int j = 1; j < 5 && i + j < list.size(); j++) {
                    sb.append(" ");
                    sb.append(list.get(i + j));
                    word.set(sb.toString());
                    context.write(word, one);
                }
            }
        }
    }

    public static class NGramReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        
        System.out.println("This is Jiachen's MapReduce program.");
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", "\t");

        Job job = new Job(conf, "NGram Generation");
        job.setJarByClass(NGram.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(NGramMap.class);
        job.setReducerClass(NGramReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        System.out.println("MapReduce job finished.");
    }
}
