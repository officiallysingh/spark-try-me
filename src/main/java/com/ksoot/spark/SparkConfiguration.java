package com.ksoot.spark;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
class SparkConfiguration {

  @Bean(name = "sparkSession", destroyMethod = "stop")
  SparkSession sparkSession() {
    return SparkSession.builder()
        .appName("spark-try-me-task")
        .master("local[1]")

        //            .config("spark.driver.extraJavaOptions", "--add-exports
        // java.base/sun.nio.ch=ALL-UNNAMED")
        //            .config("spark.executor.extraJavaOptions", "--add-exports
        // java.base/sun.nio.ch=ALL-UNNAMED")
        //
        //            .config("spark.driver.extraJavaOptions", "--add-exports
        // java.base/sun.util.calendar=ALL-UNNAME")
        //            .config("spark.executor.extraJavaOptions", "--add-exports
        // java.base/sun.util.calendar=ALL-UNNAME")

        .config("spark.sql.datetime.java8API.enabled", true)
        .config("spark.ui.enabled", true)
        .getOrCreate();
  }
  // --add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-exports
  // java.base/sun.util.calendar=ALL-UNNAMED
}
