package com.ksoot.spark;

import com.ksoot.spark.executor.FeatureFixExecutor;
import com.ksoot.spark.executor.ParquetWriterExecutor;
import com.ksoot.spark.executor.SparkBucketizeExecutor;
import com.ksoot.spark.executor.SparkUDFExecutor;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.annotation.Bean;

@Slf4j
@EnableTask
@SpringBootApplication
public class SparkTryMeTask {

  public static void main(String[] args) {
    SpringApplication.run(SparkTryMeTask.class, args);
  }

  @PostConstruct
  public void init() {
    log.info("Initialization ...");
  }

  @Bean
  public ApplicationRunner applicationRunner(
      final SparkUDFExecutor sparkUDFExecutor,
      final SparkBucketizeExecutor sparkBucketizeExecutor,
      final ParquetWriterExecutor parquetWriterExecutor,
      final FeatureFixExecutor featureFixExecutor) {
    return new SparkPipelineRunner(
        sparkUDFExecutor, sparkBucketizeExecutor, parquetWriterExecutor, featureFixExecutor);
  }

  @Slf4j
  @RequiredArgsConstructor
  public static class SparkPipelineRunner implements ApplicationRunner {

    private final SparkUDFExecutor sparkUDFExecutor;
    private final SparkBucketizeExecutor sparkBucketizeExecutor;
    private final ParquetWriterExecutor parquetWriterExecutor;
    private final FeatureFixExecutor featureFixExecutor;

    @Override
    public void run(final ApplicationArguments args) throws InterruptedException {
      //      this.sparkUDFExecutor.execute();
      //      this.sparkBucketizeExecutor.execute();
      this.parquetWriterExecutor.execute();
      //      this.parquetWriterExecutor.execute();
      //      this.featureFixExecutor.execute();
    }
  }

  //  @Bean
  //  public SparkSessionBuilderCustomizer enableHiveSupportCustomizer() {
  //    return SparkSession.Builder::enableHiveSupport;
  //  }

  //  @Bean
  //  SparkConf sparkConf() {
  //    final SparkConf sparkConf = new SparkConf();
  //    return sparkConf;
  //  }

  //  @Bean(destroyMethod = "stop")
  //  SparkSession sparkSession() {
  //    SparkConf sparkConf = new SparkConf();
  //    sparkConf.set("spark.master", "local[*]");
  //    final SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
  //    return sparkSession;
  //  }

  //  @Bean
  //  Properties sparkProperties() {
  //    Properties sparkProperties = new Properties();
  //    sparkProperties.put("spark.master", "local[*]");
  //    return sparkProperties;
  //  }

  //  @Bean
  //  @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
  //  SparkSession.Builder sparkSessionBuilder() {
  //    SparkConf sparkConf = new SparkConf();
  //    sparkConf.set("spark.master", "local[*]");
  //    SparkSession.Builder builder = SparkSession.builder().config(sparkConf);
  //    return builder;
  //  }
}
