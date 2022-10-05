import org.junit.Test;

import static org.junit.Assert.*;

public class Query5Test {
    
    @Test
    public void debug() throws Exception {
        String[] input = new String[3];
        input[0] = "hdfs://localhost:9000/project1/customers.csv";
        input[1] = "hdfs://localhost:9000/project1/transactions.csv";
        input[2] = "hdfs://localhost:9000/project1/output-query5.txt";

        Query5 wc = new Query5();
        wc.debug(input);
    }
}