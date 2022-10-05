import org.junit.Test;

public class Query5Test {
    
    @Test
    public void debug() throws Exception {
        String[] input = new String[4];
        input[0] = "hdfs://localhost:9000/project1/customers.csv";
        input[1] = "hdfs://localhost:9000/project1/transactions.csv";
        input[2] = "hdfs://localhost:9000/project1/output-query5-MR1.txt";
        input[3] = "hdfs://localhost:9000/project1/output-query5-MR2.txt";

        Query5 wc = new Query5();
        wc.debug(input);
    }
}