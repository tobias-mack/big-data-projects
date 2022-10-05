import org.junit.Test;

public class Query3Test {

    @Test
    public void debug() throws Exception {
        String[] input = new String[3];
        input[0] = "hdfs://localhost:9000/project1/customers.csv";
        input[1] = "hdfs://localhost:9000/project1/transactions.csv";
        input[2] = "hdfs://localhost:9000/project1/output-query3.txt";

        Query3 wc = new Query3();
        wc.debug(input);
    }
}