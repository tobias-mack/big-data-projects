import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Write a job(s) that reports the customers whose Age between 20 and 50 (inclusive).
 * select customer.id
 * from customer
 * where customer.age between 20 and 50
 */
public class Query1 {

    //prints out id and age
    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable customerID = new IntWritable();
        private IntWritable customerAge = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] singleCustomerData = valueString.split(",");
            if (singleCustomerData.length == 6) {
                customerID.set(Integer.parseInt(singleCustomerData[CustomerConstants.id]));
                customerAge.set(Integer.parseInt(singleCustomerData[CustomerConstants.age]));
                if (customerAge.get() >= 20 && customerAge.get() <= 50) {
                    context.write(customerID, customerAge);
                }
            } else {
                //skip if line has not correctly sorted data
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "query1");
        job.setJarByClass(Query1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
