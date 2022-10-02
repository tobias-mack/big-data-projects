/**
 * Write a job(s) that reports for every customer, the number of transactions that customer did and
 * the total sum of these transactions. The output file should have one line for each customer
 * containing:
 * CustomerID, CustomerName, NumTransactions, TotalSum
 * You are required to use a Combiner in this query
 *
 * select customer.id, customer.name, count(*), count(transaction.transTotal)
 * from customer and transaction on customer.id = transaction.id
 * group by customer.id
 * */
public class Query2 {
}
