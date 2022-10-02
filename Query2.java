import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Query2 {

    public static class CustomerMapper extends Mapper<Object, Text, Text, Text>{
        private Text customerId = new Text();
        private Text customerName = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] singleCustomerData = valueString.split(",");
            customerId.set(singleCustomerData[CustomerConstants.id]);
            customerName.set(singleCustomerData[CustomerConstants.name]);

            context.write(customerId, customerName);
        }
    }

    public static class TransactionMapper extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private Text customerID = new Text();
        private Text transactionTotal = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] singleTransactionData = valueString.split(",");
            customerID.set(singleTransactionData[TransactionConstants.custID]);
            transactionTotal.set(singleTransactionData[TransactionConstants.transTotal]);
            context.write(customerID, transactionTotal);
        }
    }

    public static class TokenizerCombiner extends Reducer<Text, Text, Text, Text>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String customerName = "";
            double transactionSum = 0F;
            int transactionCount = 0;

            for(Text t : values){
                String[] data = t.toString().split(",");

                customerName = data[0];
                transactionCount++;
                transactionSum += Float.parseFloat(data[1]);
            }

            context.write(key, new Text(customerName+","+transactionCount+","+transactionSum));
        }

    }

    public static class TokenizerReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String customerName = "";
            double transactionSum = 0F;
            int transactionCount = 0;

            for(Text t : values){
                String[] data = t.toString().split(",");

                customerName = data[0];
                transactionCount += Integer.parseInt(data[1]);
                transactionSum += Float.parseFloat(data[2]);
            }

            context.write(key, new Text(customerName+","+transactionCount+","+transactionSum));

        }
    }

    public void debug(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C://hadoop-3.3.4//");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "query2");
        job.setJarByClass(Query2.class);
        //job.setMapperClass(Query2.CustomerMapper.class);
        //job.setMapperClass(Query2.TransactionMapper.class);
        job.setCombinerClass(TokenizerCombiner.class);
        job.setReducerClass(TokenizerReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        /*FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));*/

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "query2");
        job.setJarByClass(Query2.class);
        //job.setMapperClass(Query2.CustomerMapper.class);
        //job.setMapperClass(Query2.TransactionMapper.class);
        job.setCombinerClass(TokenizerCombiner.class);
        job.setReducerClass(TokenizerReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        /*FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));*/

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
