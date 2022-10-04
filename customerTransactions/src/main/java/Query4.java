import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

public class Query4 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private final HashMap<Integer, Integer> map = new HashMap<>();

        public void setup(Context context) throws IOException{
            String[] splitLine;
            URI[] uris = context.getCacheFiles();

            FSDataInputStream customerData = FileSystem.get((context).getConfiguration()).open(new Path(uris[0]));
            BufferedReader bufferReader = new BufferedReader(new InputStreamReader(customerData));
            String line = bufferReader.readLine();
            while (line != null){
                splitLine = line.split(",");
                int customerID = Integer.parseInt(splitLine[0]);
                int countryCode = Integer.parseInt(splitLine[4]);
                map.put(customerID, countryCode);
                line = bufferReader.readLine();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Text cc = new Text();
            FloatWritable trans = new FloatWritable();
            String line = value.toString();
            String[] splitLine = line.split(",");
            int customerID = Integer.parseInt(splitLine[1]);
            String transTotal = splitLine[2];
            int countryCode = map.get(customerID);
//            trans.set(Float.parseFloat(transTotal.trim()));
            cc.set(countryCode+"");
            context.write(cc, new Text(transTotal));
        }
    }

    public static class TokenizerReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> iterator, Context context) throws IOException, InterruptedException {
            float transMin = 1000F, transMax = 0F, transVal;
            int count = 0;
            String[] values;
            int countryCode = 0;
            for (Text val : iterator) {
                count++;
                values = val.toString().split(",");

                if(values[0].equals("transaction")){
                    transVal = Float.parseFloat(values[3]);
                    if (transVal < transMin) {
                        transMin = transVal;
                    }
                    else if (transVal > transMax) {
                        transMax = transVal;
                    }
                }
                else{
                    countryCode = Integer.parseInt(values[1]);
                }

            }

            Text output = new Text();
            output.set(", "+countryCode+", " + count + ", " + transMin + ", " + transMax);
            context.write(key, output);
        }
    }

    public void debug(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        System.setProperty("hadoop.home.dir", "C://hadoop-3.3.4//");

        Job job = Job.getInstance(new Configuration(), "Query 3");
        job.setJarByClass(Query4.class);
        job.addCacheFile(new Path(args[0]).toUri());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(Query4.TokenizerMapper.class);
        job.setReducerClass(Query4.TokenizerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C://hadoop-3.3.4//");
        Configuration conf = new Configuration();

        Job job = Job.getInstance(new Configuration(), "Query 3");
        job.setJarByClass(Query4.class);
        job.addCacheFile(new Path(args[0]).toUri());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(Query4.TokenizerMapper.class);
        job.setReducerClass(Query4.TokenizerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
