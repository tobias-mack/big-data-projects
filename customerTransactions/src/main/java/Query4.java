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

    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private final HashMap<Integer, Integer> map = new HashMap<>();
        private IntWritable countryCode = new IntWritable();
        private IntWritable transactionTotal = new IntWritable();

        public void setup(Context context) throws IOException {
            String[] splitLine;
            URI[] files = context.getCacheFiles();

            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path getFilePath = new Path(files[0].toString());
            BufferedReader read = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
            String line = read.readLine();

            while (line != null) {
                splitLine = line.split(",");
                int customerID = Integer.parseInt(splitLine[CustomerConstants.id]);
                int countryCode = Integer.parseInt(splitLine[CustomerConstants.countryCode]);
                map.put(customerID, countryCode);
                line = read.readLine();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splitLine = line.split(",");

            int customerID = Integer.parseInt(splitLine[TransactionConstants.custID]);
            countryCode.set(map.get(customerID));
            transactionTotal.set(Integer.parseInt(splitLine[TransactionConstants.transTotal]));

            context.write(countryCode, transactionTotal);
        }
    }

    public static class TokenizerReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int transMin = 1000, transMax = 0;
            int customerCount = 0;

            for (IntWritable t : values) {
                int transTotal = t.get();
                customerCount++; //TODO: customercount not correct yet ! duplications through cashing

                if (transTotal < transMin) {
                    transMin = transTotal;
                }
                if (transTotal > transMax) {
                    transMax = transTotal;
                }

            }

            String string = String.format("CountryCode: %d\t Customer: %d\tMinTransTotal: %d\tMaxTransTotal: %d",
                    key.get(), customerCount, transMin, transMax);
            context.write(key, new Text((string)));
        }
    }

    public void debug(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        System.setProperty("hadoop.home.dir", "/home/twobeers/Downloads/hadoop-3.3.4");
        //System.setProperty("hadoop.home.dir", "C://hadoop-3.3.4//");

        Job job = Job.getInstance(new Configuration(), "Query4");
        job.setJarByClass(Query4.class);
        job.addCacheFile(new Path(args[0]).toUri());

        job.setMapOutputKeyClass(IntWritable.class);
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

        Job job = Job.getInstance(new Configuration(), "Query4");
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
