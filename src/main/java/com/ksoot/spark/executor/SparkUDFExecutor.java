package com.ksoot.spark.executor;

import static org.apache.spark.sql.functions.*;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.ksoot.spark.Dataframe;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
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
    //    originalDf.printSchema();
    //    originalDf.show();

    Dataset<Row> customerMinMaxDateDf =
        originalDf
            .groupBy("customer_id")
            .agg(min("date").as("min_date"), max("date").as("max_date"));
    //    customerMinMaxDateDf.printSchema();
    //    customerMinMaxDateDf.show();

    // Register a UDF to generate a sequence of dates
    this.sparkSession
        .udf()
        .register(
            "explodeDateSeq",
            (final LocalDate start, final LocalDate end) -> {
              final long numOfDaysBetween =
                  ChronoUnit.DAYS.between(start, end) + 1; // +1 to include end date
              final List<LocalDate> dateList =
                  Stream.iterate(start, date -> date.plusDays(1))
                      .limit(numOfDaysBetween)
                      .collect(Collectors.toList());
              return dateList;
            },
            DataTypes.createArrayType(DataTypes.DateType));

    // Generate the expanded dataset
    Dataset<Row> customerIdDatesDf =
        customerMinMaxDateDf
            .withColumn(
                "date",
                functions.explode(
                    callUDF(
                        "explodeDateSeq",
                        customerMinMaxDateDf.col("min_date"),
                        customerMinMaxDateDf.col("max_date"))))
            .select("customer_id", "date");

//    customerIdDatesDf.printSchema();
//    customerIdDatesDf.show();
//    originalDf.printSchema();
//    originalDf.show();

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
