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

/**
 * Assume we want to design an analytics task on the data as follows:
 * <p>
 * 1) The Age attribute is divided into six groups, which are [10, 20), [20, 30), [30, 40), [40, 50),
 * [50, 60), and [60, 70]. The bracket “[“ means the lower bound of a range is included, whereas “)”
 * means the upper bound of a range is excluded.
 * <p>
 * 2) Within each of the above age ranges, further division is performed based on the “Gender”,
 * i.e., each of the 6 age groups is further divided into two groups.
 * <p>
 * 3) For each group, we need to report the following info:
 * Age Range, Gender, MinTransTotal, MaxTransTotal, AvgTransTotal
 */
public class Query5 {
    //custValues needed: id, age, gender
    //transValues needed: custId, transtotal

    //Map1: key: id         value: age, gender  |  transtotal
    //reduce: iterate over transactions of singleCustomer
    // ->key: id  value: age, gender, min,max,total of singlecust
    //
    // Map2: key: ageGroups 1,2,3,4,5,6 + gender => 10-19 - Male,...  value: min,max,total
    //reduce2: iterate over transaction values
    //-> Age Range, Gender, MinTransTotal, MaxTransTotal, AvgTransTotal


    public static class CustomerMapper extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable customerID = new IntWritable();
        private Text age = new Text();
        private Text gender = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] singleCustomerData = valueString.split(",");
            customerID.set(Integer.parseInt(singleCustomerData[CustomerConstants.id]));
            age.set(singleCustomerData[CustomerConstants.age]);
            gender.set(singleCustomerData[CustomerConstants.gender]);

            context.write(customerID, new Text("customer," + age + "," + gender));
        }
    }

    public static class TransactionMapper extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable customerID = new IntWritable();
        private Text transactionTotal = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] singleTransactionData = valueString.split(",");
            customerID.set(Integer.parseInt(singleTransactionData[TransactionConstants.custID]));
            transactionTotal.set(singleTransactionData[TransactionConstants.transTotal]);

            context.write(customerID, new Text("transaction," + transactionTotal));
        }
    }

    public static class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //iterate over transactions of singleCustomer
            // ->key: id  value: age, gender, min,max,total of singlecust
            String age = "";
            String gender = "";
            int minTotal = 10000;
            int maxTotal = -1;
            int transactionsCount = 0;
            int transTotalSum = 0;

            for (Text t : values) {
                String[] data = t.toString().split(",");

                if (data[0].equals("transaction")) {
                    int transTotal = Integer.parseInt(data[1]);
                    transactionsCount++;
                    transTotalSum += transTotal;

                    if (transTotal < minTotal) {
                        minTotal = transTotal;
                    }
                    if (transTotal > maxTotal) {
                        maxTotal = transTotal;
                    }

                } else if (data[0].equals("customer")) {
                    age = data[1];
                    gender = data[2];
                }
            }

            String string = String.format(",%s,%s,%d,%d,%d,%d", age, gender, minTotal, maxTotal, transactionsCount, transTotalSum);
            context.write(key, new Text((string)));
        }

    }

    // MapReduce Job 2
    public static class AgeMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] singleCustomer = valueString.split(",");
            //id = singleCustomer[0] not longer needed
            int age = Integer.parseInt(singleCustomer[1]);
            String ageRange = getAgeRange(age);
            String gender = singleCustomer[2];
            String minTotal = singleCustomer[3];
            String maxTotal = singleCustomer[4];
            String transactionsCount = singleCustomer[5];
            String transTotalSum = singleCustomer[6];

            Text newKey = new Text(String.format("%s: %s", ageRange, gender));

            context.write(newKey, new Text(minTotal + "," + maxTotal + "," + transactionsCount + "," + transTotalSum));
        }

        private String getAgeRange(int age) {
            if (9 < age && age < 20) {
                return "10-19";
            } else if (19 < age && age < 30) {
                return "20-29";
            } else if (29 < age && age < 40) {
                return "30-39";
            } else if (39 < age && age < 50) {
                return "40-49";
            } else if (49 < age && age < 60) {
                return "50-59";
            } else if (59 < age && age < 70) {
                return "60-69";
            }
            return "age<20||age>70";
        }
    }

    public static class AgeReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //reduce2: iterate over transaction values
            //-> Age Range, Gender, MinTransTotal, MaxTransTotal, AvgTransTotal
            int minTotal_InAgeRange = 10000;
            int maxTotal_InAgeRange = 0;
            int transactionsCount_InAgeRange = 0;
            int transTotalSum_InAgeRange = 0;

            for (Text t : values) {
                String[] data = t.toString().split(",");
                //singleCustomerData
                int minTotal = Integer.parseInt(data[0]);
                int maxTotal = Integer.parseInt(data[1]);
                int transactionsCount = Integer.parseInt(data[2]);
                int transTotalSum = Integer.parseInt(data[3]);

                transactionsCount_InAgeRange += transactionsCount;
                transTotalSum_InAgeRange += transTotalSum;

                if (minTotal < minTotal_InAgeRange) {
                    minTotal_InAgeRange = minTotal;
                }
                if (maxTotal > maxTotal_InAgeRange) {
                    maxTotal_InAgeRange = maxTotal;
                }

            }

            double avgTransTotal = (double) transTotalSum_InAgeRange / transactionsCount_InAgeRange;


            String string = String.format("%d\t%d\t%f", minTotal_InAgeRange, maxTotal_InAgeRange, avgTransTotal);
            context.write(key, new Text((string)));
        }

    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query5-Join");
        job.setJarByClass(Query5.class);

        job.setReducerClass(Query5.JoinReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Query5.CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Query5.TransactionMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);

        // MapReduce Job 2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Query5");
        job2.setJarByClass(Query5.class);

        job2.setMapperClass(Query5.AgeMapper.class);
        job2.setReducerClass(Query5.AgeReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

    }

}
