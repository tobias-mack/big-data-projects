import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Write a job(s) that joins the Customers and Transactions datasets (based on the customer ID)
 * and reports for each customer the following info:
 * <p>
 * CustomerID, Name, Salary, NumOfTransactions, TotalSum, MinItems
 * <p>
 * Where NumOfTransactions is the total number of transactions done by the customer, TotalSum
 * is the sum of field “TransTotal” for that customer, and MinItems is the minimum number of items
 * in transactions done by the customer.
 */

public class Query3 {

    public static class CustomerMapper extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable customerID = new IntWritable();
        private Text customerName = new Text();
        private IntWritable salary = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] singleCustomerData = valueString.split(",");
            customerID.set(Integer.parseInt(singleCustomerData[CustomerConstants.id]));
            customerName.set(singleCustomerData[CustomerConstants.name]);
            salary.set(Integer.parseInt(singleCustomerData[CustomerConstants.salary]));
            context.write(customerID, new Text("customer," + customerName + "," + salary));
        }
    }

    public static class TransactionMapper extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable customerID = new IntWritable();
        private Text transactionTotal = new Text();
        private IntWritable transNumItems = new IntWritable();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] singleTransactionData = valueString.split(",");
            customerID.set(Integer.parseInt(singleTransactionData[TransactionConstants.custID]));
            transactionTotal.set(singleTransactionData[TransactionConstants.transTotal]);
            transNumItems.set(Integer.parseInt(singleTransactionData[TransactionConstants.transNumItems]));
            context.write(customerID, new Text("transaction," + transactionTotal + "," + transNumItems));
        }
    }

    public static class MapReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //CustomerID, Name, Salary, NumOfTransactions, TotalSum, MinItems
            String customerName = "";
            int salary = 0;
            int transactionCount = 0;
            int transactionSum = 0;
            int minItems = 10000;

            for (Text t : values) {
                String[] data = t.toString().split(",");

                if (data[0].equals("transaction")) {
                    transactionCount++;
                    transactionSum += Integer.parseInt(data[1]);
                    int items = Integer.parseInt(data[2]);
                    if(items < minItems){
                        minItems = items;
                    }
                } else if (data[0].equals("customer")) {
                    customerName = data[1];
                    salary = Integer.parseInt(data[2]);
                }
            }

            String string = String.format("%s\t%d\t%d\t%d\t%d", customerName, salary, transactionCount, transactionSum, minItems);
            context.write(key, new Text((string)));
        }

    }


    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query3");
        job.setJarByClass(Query3.class);

        job.setReducerClass(Query3.MapReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Query3.CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Query3.TransactionMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query3");
        job.setJarByClass(Query3.class);

        job.setReducerClass(Query3.MapReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Query3.CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Query3.TransactionMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
