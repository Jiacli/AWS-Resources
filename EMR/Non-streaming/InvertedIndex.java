import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InvertedIndex {

    public static class Map extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // read in filename
            FileSplit fs = (FileSplit) context.getInputSplit();
            String location = fs.getPath().getName();
            Text file = new Text(location);

            Set<String> hashset = new HashSet<String>();

            String[] tokenizer = value.toString().split("\\W+");

            // map
            for (String s : tokenizer) {
                if (hashset.contains(s.toLowerCase()))
                    continue;

                hashset.add(s.toLowerCase());
                word.set(s.toLowerCase());
                context.write(word, file);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String word = key.toString();
            
            // check whether belong to stop words (bonus)
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
                    .getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    new FileInputStream(cacheFiles[0].toString())));
            
            String line = null;
            while ((line = br.readLine()) != null) {
                if (line.equals(word)) {
                    br.close();
                    return;
                }
            }
            br.close();

            // extract filename
            HashSet<String> hashset = new HashSet<String>();
            StringBuffer sb = new StringBuffer();

            for (Text val : values) {
                String s = val.toString();
                if (hashset.contains(s)) {
                    continue;
                }
                hashset.add(s);
                sb.append(" ");
                sb.append(val.toString());
            }

            context.write(new Text(word + " "), new Text(sb.toString()));
        }

    }

    public static void main(String[] args) throws Exception {

        System.out.println("This is Jiachen's MapReduce program.");
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ":");

        Job job = new Job(conf, "Inverted Index with Bonus");
        job.setJarByClass(InvertedIndex.class);

        DistributedCache.addCacheFile(new URI("hdfs:/english.stop.txt"),
                job.getConfiguration());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        System.out.println("MapReduce job finished.");
    }
}
