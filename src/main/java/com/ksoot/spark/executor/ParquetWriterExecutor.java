package com.ksoot.spark.executor;

import java.sql.Timestamp;
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
public class ParquetWriterExecutor {

  private final SparkSession sparkSession;

  public void execute() {
    // Define schema
    //        StructType schema = DataTypes.createStructType(new StructField[]{
    //                DataTypes.createStructField("event_timestamp", DataTypes.TimestampType,
    // false),
    //                DataTypes.createStructField("driver_id", DataTypes.LongType, false),
    //                DataTypes.createStructField("conv_rate", DataTypes.DoubleType, false),
    //                DataTypes.createStructField("acc_rate", DataTypes.DoubleType, false),
    //                DataTypes.createStructField("avg_daily_trips", DataTypes.LongType, false),
    //                DataTypes.createStructField("created", DataTypes.TimestampType, false)
    //        });
    //
    //        // Create data
    //        String[] data = new String[]{
    //                "2024-08-24 21:00:00.000000000,1001,0.15346666,0.5661219,27,2024-08-25
    // 21:52:25.824",
    //                "2024-08-24 21:04:50.000000000,1001,0.16400011,0.7121123,20,2024-08-25
    // 21:52:25.824",
    //                "2024-08-25 22:60:00.000000000,1001,0.66012345,0.4201201,25,2024-08-25
    // 21:52:25.824",
    //                "2024-08-26 23:00:00.000000000,1001,0.80000055,0.3201205,35,2024-08-25
    // 21:52:25.824",
    //                "2024-08-25 21:00:00.000000000,1002,0.9487353,0.6740312,212,2024-08-25
    // 21:52:25.824",
    //                "2024-08-26 22:00:00.000000000,1002,0.34636292,0.65501297,710,2024-08-25
    // 21:52:25.824",
    //                "2024-08-25 21:00:00.000000000,1003,0.38642398,0.72331494,314,2024-08-25
    // 21:52:25.824",
    //                "2024-08-26 22:00:00.000000000,1003,0.53867257,0.032498427,279,2024-08-25
    // 21:52:25.824",
    //                "2024-08-26 09:00:00.000000000,1003,0.15253077,0.5844595,877,2024-08-25
    // 21:52:25.824",
    //                "2024-08-25 21:00:00.000000000,1004,0.09679014,0.87793076,707,2024-08-25
    // 21:52:25.824",
    //                "2024-08-25 22:00:00.000000000,1004,0.5551066,0.57531226,814,2024-08-25
    // 21:52:25.824",
    //                "2024-08-26 23:00:00.000000000,1004,0.542623,0.709294,269,2024-08-25
    // 21:52:25.824",
    //                "2024-08-26 00:00:00.000000000,1004,0.18909124,0.48615456,224,2024-08-25
    // 21:52:25.824"
    //        };
    //
    //        // Create Dataset<Row> from data
    //        List<Row> rows = Arrays.stream(data)
    //                .map(line -> {
    //                    String[] parts = line.split(",");
    //                    return RowFactory.create(
    //                            Timestamp.valueOf(parts[0]),
    //                            Long.parseLong(parts[1]),
    //                            Double.parseDouble(parts[2]),
    //                            Double.parseDouble(parts[3]),
    //                            Long.parseLong(parts[4]),
    //                            Timestamp.valueOf(parts[5])
    //                    );
    //                })
    //                .toList();
    //
    //        Dataset<Row> df = sparkSession.createDataFrame(rows, schema);
    //        df.printSchema();
    //        df.show(false);

    // Write DataFrame to Parquet
    //        df.coalesce(1);
    //        df.write().parquet("spark-output");

    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("/identifier/patient/id", DataTypes.StringType, true),
              DataTypes.createStructField("/attribute/patient/sex", DataTypes.StringType, true),
              DataTypes.createStructField("/attribute/patient/age", DataTypes.IntegerType, true),
              DataTypes.createStructField(
                  "/attribute/patient/cholesterol", DataTypes.FloatType, true),
              DataTypes.createStructField("/attribute/patient/dob", DataTypes.TimestampType, true),
              DataTypes.createStructField("/attribute/patient/label", DataTypes.StringType, true),
              DataTypes.createStructField(
                  "/property/patient/is_diabitic", DataTypes.BooleanType, true),
              DataTypes.createStructField("/property/patient/salary", DataTypes.DoubleType, true),
              DataTypes.createStructField("/property/patient/pin", DataTypes.LongType, true)
            });

    List<Row> rows =
        Arrays.asList(
            RowFactory.create(
                "1",
                "MALE",
                25,
                Float.parseFloat("260.9"),
                Timestamp.valueOf("2016-12-31 03:22:34"),
                "3",
                true,
                34900.00,
                Long.parseLong("238492")),
            RowFactory.create(
                "2",
                "FEMALE",
                45,
                Float.parseFloat("251.5"),
                Timestamp.valueOf("2016-12-31 03:22:34"),
                "2",
                true,
                56000.00,
                Long.parseLong("122002")),
            RowFactory.create(
                "3",
                "NULL",
                46,
                Float.parseFloat("290.4"),
                Timestamp.valueOf("2016-12-31 03:21:21"),
                "3",
                false,
                78000.00,
                Long.parseLong("122003")),
            RowFactory.create(
                "4",
                "MALE",
                15,
                Float.parseFloat("146.9"),
                Timestamp.valueOf("2015-4-21 14:32:21"),
                "2",
                true,
                134000.00,
                Long.parseLong("122004")),
            RowFactory.create(
                "5",
                "NOT_SPECIFIED",
                34,
                null,
                Timestamp.valueOf("2015-4-21 19:23:20"),
                "1",
                false,
                123000.00,
                Long.parseLong("122005")));

    //        // Create a DataFrame with the sample data and schema
    //        Dataset<Row> df = sparkSession.createDataFrame(data, schema);
    //
    //        // Path to save the Parquet file
    //        String outputPath = "src/main/resources/userdata.parquet";
    //
    //        // Write the DataFrame to a Parquet file
    //        df.write().mode(SaveMode.Overwrite).parquet(outputPath);

    Dataset<Row> df = sparkSession.createDataFrame(rows, schema);
    df.printSchema();
    df.show(false);

    // Write DataFrame to Parquet
    df.coalesce(1);
    df.write().parquet("spark-output");
  }
}
