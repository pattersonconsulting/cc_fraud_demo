
import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types._

import org.pmml4s.model.Model
import org.pmml4s.data.Series

import spray.json._

import scala.io.Source

import java.text.SimpleDateFormat

object FeatureEngineeringTest {

  def main(args: Array[String]): Unit = {
    
    Console.println("\n=== Creating the session ===\n")

    val session = Session.builder.configFile("conn.properties").create

    val df_cust_txns = session.table("CUSTOMER_CC_TRANSACTIONS") //.select(col())
    df_cust_txns.show()

    val num_rows_txns = df_cust_txns.count()

    Console.println("\nNumber of Rows in CC Transtions Table: " + num_rows_txns)

/*

0 if transaction between 6am and 0pm, 1 if transaction between 0pm and 6am. The new feature is called TX_DURING_NIGHT.
*/

    val dfDateTimeFeat = df_cust_txns.withColumns(
        Seq("TX_DURING_WEEKEND", "TX_DURING_NIGHT"), 
        Seq(
          iff(
            dayofweek(col("TX_DATETIME")) === 6 || dayofweek(col("TX_DATETIME")) === 0, 
            lit(1), 
            lit(0)
          ), 
          iff(hour(col("TX_DATETIME")) <= 6 || hour(col("TX_DATETIME")) > 23, lit(1), lit(0)
        )
      ))

    dfDateTimeFeat.show()



    val num_rows_txns_datetimefeat = dfDateTimeFeat.count()

    Console.println("\nNumber of Rows in CC Transtions Table (after date time feature): " + num_rows_txns_datetimefeat)    
    // Number of Rows in CC Transtions Table (after date time feature): 1754155 (this is right)


    // The next step is to create the behaviour based features,

    /*
Original Paper


        The first feature will be the number of transactions that occur within a time window (Frequency). 


        The second will be the average amount spent in these transactions (Monetary value). 


The time windows will be set to one, seven, and thirty days. 


This will generate six new features.

Python:


    # For each window size
    for window_size in windows_size_in_days:
        
        # Compute the sum of the transaction amounts and the number of transactions for the given window size
        SUM_AMOUNT_TX_WINDOW=customer_transactions['TX_AMOUNT'].rolling(str(window_size)+'d').sum()
        NB_TX_WINDOW=customer_transactions['TX_AMOUNT'].rolling(str(window_size)+'d').count()
    
        # Compute the average transaction amount for the given window size
        # NB_TX_WINDOW is always >0 since current transaction is always included
        AVG_AMOUNT_TX_WINDOW=SUM_AMOUNT_TX_WINDOW/NB_TX_WINDOW
    
        # Save feature values
        customer_transactions['CUSTOMER_ID_NB_TX_'+str(window_size)+'DAY_WINDOW']=list(NB_TX_WINDOW)
        customer_transactions['CUSTOMER_ID_AVG_AMOUNT_'+str(window_size)+'DAY_WINDOW']=list(AVG_AMOUNT_TX_WINDOW)
    

    */

    /*

    calculate the number of transactions and average amount on a day level, 
    but I need to store it on a transaction level. 
    The calculations is to be based on previous 
      one, seven and thirty days 
    and also including the transactions done so far during the current day of a transaction.

  divide work into multiple steps and that is something that is easy to do using a programming language and the Snowpark API.

  (1.) Generate a dataframe with one row for each customer and day that is between the minimum and maximum date in our data.
  (2.) Calculate the number of transaction and amount by customer and day, adding zeros for those days the customer has no transactions.
  (3.) Using Snowflake window functions to calculate the number of transaction and average amount during previous one, seven and thirty days, excluding the current day.
  (4.) Join it with the transactions and also calculate the transactions and amount during the current day.

    */


    val dateInfo = df_cust_txns.select(
      min(col("TX_DATETIME")).alias("END_DATE"), 
      datediff("DAY", col("END_DATE"), max(col("TX_DATETIME")))
    ).collect()    


    
    val dStartDate = new SimpleDateFormat("yyyy-MM-dd").format(dateInfo(0).getTimestamp(0))
    val nDays = dateInfo(0).getInt(1)

    Console.println("\nStart Date: " + dStartDate)
    Console.println("nDays: " + nDays)

    // CORRECT DATES to this point



/*
    Now I can define a new dataframe, dfDays, that will return one row for each day that is between the min and max dates in our transactions.

*/


    val dfDays = session.range(nDays)
                       .withColumn("TX_DATE", 
                            to_date(dateadd("DAY", callBuiltin("SEQ4"), lit(dStartDate))))


    val num_rows_txns_dfDays = dfDays.count()

    Console.println("\nNumber of Rows in CC Transtions Table (after num_rows_txns_dfDays feature): " + num_rows_txns_dfDays + "\n")   
// # Number of Rows in CC Transtions Table (after num_rows_txns_dfDays feature): 182



    val dfCustomers = session.table("CUSTOMERS").select("CUSTOMER_ID")

    dfCustomers.show()


    val num_rows_txns_dfCustomers = dfCustomers.count()

    Console.println("\nNumber of Rows in dfCustomers (after num_rows_txns_dfCustomers feature): " + num_rows_txns_dfCustomers + "\n")   
// Number of Rows in dfCustomers (after num_rows_txns_dfCustomers feature): 4990
// this is correct


    val dfCustDay = dfDays.join(dfCustomers)

    dfCustDay.show()


    val num_rows_txns_dfCustDay = dfCustDay.count()

    Console.println("\nNumber of Rows in dfCustDay (after num_rows_txns_dfCustDay feature): " + num_rows_txns_dfCustDay + "\n")   

// Number of Rows in dfCustDay (after num_rows_txns_dfCustDay feature): 908180 (== 4990 x 182)

    val zeroifnull = builtin("ZEROIFNULL")

    // df_cust_txns: the original table of raw transactions
    // dfCustDay: the matrix of customers and possible dates

    val dfCustTrxByDay = df_cust_txns.join(dfCustDay, df_cust_txns("CUSTOMER_ID") === dfCustDay("CUSTOMER_ID")
                                        && to_date(df_cust_txns("TX_DATETIME")) === dfCustDay("TX_DATE"), "rightouter")

                                       .select(dfCustDay("CUSTOMER_ID").alias("CUSTOMER_ID"), 
                                               dfCustDay("TX_DATE"), 
                                               zeroifnull(df_cust_txns("TX_AMOUNT")).as("TX_AMOUNT"), 
                                               iff(col("TX_AMOUNT") > 0, lit(1), lit(0)).as("NO_TRX"))

                                        .groupBy(col("CUSTOMER_ID"), col("TX_DATE"))

                                        .agg(
                                            sum(col("TX_AMOUNT")).alias("TOT_AMOUNT"), 
                                             sum(col("NO_TRX")).alias("NO_TRX")
                                         )    


    val num_rows_txns_dfCustTrxByDay = dfCustTrxByDay.count()
    Console.println("\nNumber of Rows in dfCustTrxByDay: " + num_rows_txns_dfCustTrxByDay)        
    // Number of Rows in CC Transtions Table (after dfCustTrxByDay feature): 908180


    val dfSubset = dfCustTrxByDay.filter(col("CUSTOMER_ID") === lit(0)).sort(col("TX_DATE"))
    dfSubset.show(100)    

    // NOTE: up to here, the sub-totals in dfCustTrxByDay seem correct

    // NOTE: anything we generate has to join BACK to the original transaction list


    // NOTE: I STOPPED HERE ---- NEED TO FURTHER TRIAGE THE TRANSFORMS
    // NOT SURE THE JOIN WORKS RIGHT


    
    //val rows = dfCustTrxByDay.sort(col("CUSTOMER_ID")) //.first(100)
    //rows.show(100)

    // Create a DataFrame containing a subset of the cached data.
    //val dfSubset = dfCustTrxByDay.filter(col("CUSTOMER_ID") === lit(1486)).sort(col("TX_DATE"))
    //dfSubset.show(1000)    

    val custDate = Window.partitionBy(col("customer_id")).orderBy(col("TX_DATE"))
    val win7dCust = custDate.rowsBetween(-7, -1)
    val win30dCust = custDate.rowsBetween(-30, -1)

    val dfCustFeatDay = dfCustTrxByDay
                               .select(col("TX_DATE"),col("CUSTOMER_ID"),
                                       col("NO_TRX"), col("TOT_AMOUNT"),
                                       lag(col("NO_TRX"),1).over(custDate).as("CUST_TX_PREV_1"),
                                       sum(col("NO_TRX")).over(win7dCust).as("CUST_TX_PREV_7"),
                                       sum(col("NO_TRX")).over(win30dCust).as("CUST_TX_PREV_30"),
                                       lag(col("TOT_AMOUNT"),1).over(custDate).as("CUST_TOT_AMT_PREV_1"),
                                       sum(col("TOT_AMOUNT")).over(win7dCust).as("CUST_TOT_AMT_PREV_7"),
                                       sum(col("TOT_AMOUNT")).over(win30dCust).as("CUST_TOT_AMT_PREV_30"))    

    dfCustFeatDay.show()


    val num_rows_dfCustFeatDay = dfCustFeatDay.count()
    Console.println("\nNumber of Rows in dfCustFeatDay: " + num_rows_dfCustFeatDay)    
    
    // Number of Rows in dfCustFeatDay: 908180  
    // this is the CustAccts x Days number, so far


    val winCurrDate = Window.partitionBy(col("PARTITION_KEY"))
                            .orderBy(col("TX_DATETIME"))
                            .rangeBetween(Window.unboundedPreceding, 
                                          Window.currentRow)

/*
Previously we created dfDateTimeFeat

    Number of Rows in CC Transtions Table (after date time feature): 1754155


joining it to:

dfCustFeatDay
    Number of Rows in dfCustFeatDay: 908180  

Result:

    Number of Rows in dfCustBehaviurFeat: 1226990


*/

    Console.println("\ndfDateTimeFeat:")        
    dfDateTimeFeat.show()
    

    val num_rows_txns_datetimefeat_2 = dfDateTimeFeat.count()
    Console.println("\nNumber of Rows in dfDateTimeFeat: " + num_rows_txns_datetimefeat_2)   

    Console.println("\ndfCustFeatDay:")        
    dfCustFeatDay.show()


    // FIXED: it was defaulting to "inner join", needed to explicitly specify "left" for "left outer join"
    val dfCustBehaviurFeat = dfDateTimeFeat
                              .join(dfCustFeatDay, dfDateTimeFeat.col("CUSTOMER_ID") === dfCustFeatDay.col("CUSTOMER_ID") 
                                     && to_date(dfDateTimeFeat.col("TX_DATETIME")) === dfCustFeatDay.col("TX_DATE"), "left")                             
                              .withColumn("PARTITION_KEY",
                                          concat(dfDateTimeFeat("CUSTOMER_ID"), to_date(dfDateTimeFeat("TX_DATETIME"))))
                              .withColumns(Seq("CUR_DAY_TRX",
                                                "CUR_DAY_AMT"),
                                           Seq(count(dfDateTimeFeat("CUSTOMER_ID")).over(winCurrDate), 
                                               sum(dfDateTimeFeat("TX_AMOUNT")).over(winCurrDate)))
                              .select(dfDateTimeFeat("TRANSACTION_ID"), 
                                      dfDateTimeFeat("CUSTOMER_ID").alias("CUSTOMER_ID"), 
                                      dfDateTimeFeat("TERMINAL_ID"),
                                      dfDateTimeFeat("TX_DATETIME").alias("TX_DATETIME"), 
                                      dfDateTimeFeat("TX_AMOUNT"),
                                      dfDateTimeFeat("TX_TIME_SECONDS"),
                                      dfDateTimeFeat("TX_TIME_DAYS"), 
                                      dfDateTimeFeat("TX_FRAUD"),
                                      dfDateTimeFeat("TX_FRAUD_SCENARIO"),
                                      dfDateTimeFeat("TX_DURING_WEEKEND"), 
                                      dfDateTimeFeat("TX_DURING_NIGHT"),
                                      (zeroifnull(dfCustFeatDay("CUST_TX_PREV_1")) + col("CUR_DAY_TRX")).as("CUST_CNT_TX_1"),
                                      (zeroifnull(dfCustFeatDay("CUST_TOT_AMT_PREV_1")) + col("CUR_DAY_AMT") / col("CUST_CNT_TX_1")).as("CUST_AVG_AMOUNT_1"), 
                                      (zeroifnull(dfCustFeatDay("CUST_TX_PREV_7")) + col("CUR_DAY_TRX")).as("CUST_CNT_TX_7"),
                                      (zeroifnull(dfCustFeatDay("CUST_TOT_AMT_PREV_7")) + col("CUR_DAY_AMT") / col("CUST_CNT_TX_7")).as("CUST_AVG_AMOUNT_7"),
                                      (zeroifnull(dfCustFeatDay("CUST_TX_PREV_30")) + col("CUR_DAY_TRX")).as("CUST_CNT_TX_30"),
                                      (zeroifnull(dfCustFeatDay("CUST_TOT_AMT_PREV_30")) + col("CUR_DAY_AMT") / col("CUST_CNT_TX_30")).as("CUST_AVG_AMOUNT_30"))


    dfCustBehaviurFeat.show()

    val num_rows_dfCustBehaviurFeat = dfCustBehaviurFeat.count()
    Console.println("\nNumber of Rows in dfCustBehaviurFeat: " + num_rows_dfCustBehaviurFeat)        



    Console.println("\n\nSAVING TABLE....")
    dfCustBehaviurFeat.write.mode(SaveMode.Overwrite).saveAsTable("CUSTOMER_CC_TRANSACTION_FEATURES")


    Console.println("\n=== CLOSING the session ===\n")

    session.close();

  }
}












//Main.main()