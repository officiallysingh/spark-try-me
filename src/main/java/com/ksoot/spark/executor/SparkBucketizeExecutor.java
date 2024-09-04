package com.ksoot.spark.executor;

import java.util.Arrays;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.feature.Bucketizer;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SparkBucketizeExecutor {

  private final SparkSession sparkSession;

  public void execute() {

    // $example on$
    double[] splits = {Double.NEGATIVE_INFINITY, -0.5, 0.0, 0.5, Double.POSITIVE_INFINITY};

    List<Row> data =
        Arrays.asList(
            RowFactory.create(-999.9),
            RowFactory.create(-0.5),
            RowFactory.create(-0.3),
            RowFactory.create(0.0),
            RowFactory.create(0.2),
            RowFactory.create(999.9));
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("features", DataTypes.DoubleType, false, Metadata.empty())
            });
    Dataset<Row> dataFrame = this.sparkSession.createDataFrame(data, schema);

    Bucketizer bucketizer =
        new Bucketizer().setInputCol("features").setOutputCol("bucket_col").setSplits(splits);

    // Transform original data into its bucket index.
    Dataset<Row> bucketedData = bucketizer.transform(dataFrame);

    bucketedData =
        bucketedData.withColumn(
            "bucket_col", bucketedData.col("bucket_col").cast(DataTypes.IntegerType));

    System.out.println("########### Original Dataset ###########");
    System.out.println(
        "Bucketizer output with " + (bucketizer.getSplits().length - 1) + " buckets");
    bucketedData.show();

    System.out.println("########### Using Dynamic Expression ###########");
    Dataset<Row> result1 = this.getLabelledBucketedDfUsingDynamicExpression(bucketedData);
    result1.show();

    System.out.println("########### Using Join ###########");
    Dataset<Row> result2 = this.getLabelledBucketedDfUsingDynamicExpression(bucketedData);
    result2.show();
  }

  // Approach 1
  private Dataset<Row> getLabelledBucketedDfUsingDynamicExpression(
      final Dataset<Row> bucketedData) {
    // List of string values to replace bucket_col
    List<String> replacementList = Arrays.asList("Very Low", "Low", "Medium", "High");
    // Start with the initial when condition
    Column resultColumn = functions.lit("Unknown"); // Default value for otherwise
    // Loop through the replacement list and build the when expression dynamically
    for (int i = 0; i < replacementList.size(); i++) {
      resultColumn =
          functions
              .when(bucketedData.col("bucket_col").equalTo(i), replacementList.get(i))
              .otherwise(resultColumn);
    }
    // Apply the dynamically created column to the DataFrame
    Dataset<Row> labelledBucketedData = bucketedData.withColumn("bucket_col", resultColumn);
    return labelledBucketedData;
  }

  // Approach 2
  private Dataset<Row> getLabelledBucketedDfUsingJoin(final Dataset<Row> bucketedData) {
    // Create mapping DataFrame for bucket_col to string
    Dataset<Row> bucketLabelsDf =
        this.sparkSession.createDataFrame(
            Arrays.asList(
                RowFactory.create(0, "Very Low"),
                RowFactory.create(1, "Low"),
                RowFactory.create(2, "Medium"),
                RowFactory.create(3, "High")),
            new StructType(
                new StructField[] {
                  new StructField("bucket_col", DataTypes.IntegerType, false, Metadata.empty()),
                  new StructField("bucket_label", DataTypes.StringType, false, Metadata.empty())
                }));
    // Join the original DataFrame with the mapping DataFrame
    Dataset<Row> labelledBucketedData =
        bucketedData.join(bucketLabelsDf, "bucket_col").select("features", "bucket_label");
    // Rename the column back to bucket_col
    labelledBucketedData = labelledBucketedData.withColumnRenamed("bucket_label", "bucket_col");
    return labelledBucketedData;
  }
}
