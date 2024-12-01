package com.ksoot.spark.executor;

import static org.apache.spark.sql.functions.*;

import com.ksoot.spark.Dataframe;
import com.ksoot.spark.conf.UserDefinedFunctions;
import java.time.LocalDate;
import java.util.Arrays;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SparkUDFExecutor {

  private final SparkSession sparkSession;

  public void execute() {
    log.info("Inside TestExecutor execute.... started");
    Dataset<Row> originalDf =
        this.sparkSession.createDataFrame(
            Arrays.asList(
                Dataframe.of("c1", LocalDate.of(2024, 6, 5), "f105"),
                Dataframe.of("c1", LocalDate.of(2024, 6, 6), "f106"),
                Dataframe.of("c1", LocalDate.of(2024, 6, 7), "f107"),
                Dataframe.of("c1", LocalDate.of(2024, 6, 10), "f110"),
                Dataframe.of("c2", LocalDate.of(2024, 6, 12), "f212"),
                Dataframe.of("c2", LocalDate.of(2024, 6, 13), "f213"),
                Dataframe.of("c2", LocalDate.of(2024, 6, 15), "f215")),
            Dataframe.class);

    Dataset<Row> customerMinMaxDateDf =
        originalDf
            .groupBy("customer_id")
            .agg(min("date").as("min_date"), max("date").as("max_date"));

    // Generate the expanded dataset
    Dataset<Row> customerIdDatesDf =
        customerMinMaxDateDf
            .withColumn(
                "date",
                UserDefinedFunctions.explodeDateSeq(
                    customerMinMaxDateDf.col("min_date"), customerMinMaxDateDf.col("max_date")))
            .select("customer_id", "date");

    customerIdDatesDf.show();

    final Dataset<Row> result =
        customerIdDatesDf
            .join(
                originalDf,
                customerIdDatesDf
                    .col("customer_id")
                    .equalTo(originalDf.col("customer_id"))
                    .and(customerIdDatesDf.col("date").equalTo(originalDf.col("date"))),
                "left")
            .select(
                customerIdDatesDf.col("customer_id"),
                customerIdDatesDf.col("date"),
                col("feature"));
    result.show();
  }
}
