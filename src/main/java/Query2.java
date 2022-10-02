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

    public static class CustomerMapper extends Mapper<Object, Text, IntWritable, Text>{

        private final static IntWritable one = new IntWritable(1);
        private IntWritable customerID = new IntWritable();
        private Text customerName = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] singleCustomerData = valueString.split(",");
            customerID.set(Integer.parseInt(singleCustomerData[CustomerConstants.id]));
            customerName.set(singleCustomerData[CustomerConstants.name]);
            context.write(customerID, new Text("customers:"+customerName));
        }
    }

    public static class TransactionMapper extends Mapper<Object, Text, IntWritable, Text>{

        private final static IntWritable one = new IntWritable(1);
        private IntWritable transactionID = new IntWritable();
        private Text transactionTotal = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] singleTransactionData = valueString.split(",");
            transactionID.set(Integer.parseInt(singleTransactionData[TransactionConstants.transID]));
            transactionTotal.set(singleTransactionData[TransactionConstants.transTotal]);
            context.write(transactionID, new Text("transactions:"+transactionTotal));
        }
    }

    public static class TokenizerReducer extends Reducer<IntWritable, Iterable<Text>, IntWritable, Object>{

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String customerName = "";
            double transactionSum = 0;
            int transactionCount = 0;

            for(Text t : values){
                String[] data = t.toString().split(":");

                if(data[0].equals("transactions")){
                    transactionCount++;
                    transactionSum += Float.parseFloat(data[1]);
                }
                else if(data[0].equals("customers")){
                    customerName = data[1];
                }
            }

            String string = String.format("%d\t%f", transactionCount, transactionSum);
            context.write(key, new Text((string)));
        }

    }

    public void debug(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C://hadoop-3.3.4//");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "query2");
        job.setJarByClass(Query2.class);
        //job.setMapperClass(Query2.CustomerMapper.class);
        //job.setMapperClass(Query2.TransactionMapper.class);
        job.setCombinerClass(TokenizerReducer.class);
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
        job.setCombinerClass(TokenizerReducer.class);
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
