import org.junit.Test;

public class Query4Test {

    @Test
    public void debug() throws Exception {
        String[] input = new String[3];
        input[0] = "/home/twobeers/Desktop/bigData/project1/customersTest.csv"; //path to file to cache
        input[1] = "hdfs://localhost:9000/project1/transactionsTest.csv";
        input[2] = "hdfs://localhost:9000/project1/output-query4.txt";

        Query4 wc = new Query4();
        wc.debug(input);
    }
    
}