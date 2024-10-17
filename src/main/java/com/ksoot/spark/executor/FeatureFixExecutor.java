package com.ksoot.spark.executor;

import java.util.Arrays;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class FeatureFixExecutor {

  private final SparkSession sparkSession;

  public void execute() {

    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("4", DataTypes.StringType, true),
              DataTypes.createStructField("feature_id", DataTypes.StringType, true),
              DataTypes.createStructField("feature_value", DataTypes.IntegerType, true),
            });

    List<Row> rows =
        Arrays.asList(
            RowFactory.create("asdfafadfs", "4", 25), RowFactory.create("dfgfwweewe", "4", 35));

    Dataset<Row> df = sparkSession.createDataFrame(rows, schema);
    df.printSchema();
    df.show(false);
    df = df.withColumn("feature_id", functions.lit("aaa"));
    df.printSchema();
    df.show(false);
  }
}
