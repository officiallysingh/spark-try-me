package com.ksoot.spark;

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
  public ApplicationRunner applicationRunner(final SparkUDFExecutor sparkUDFExecutor,
                                             final SparkBucketizeExecutor sparkBucketizeExecutor) {
    return new SparkPipelineRunner(sparkUDFExecutor, sparkBucketizeExecutor);
  }

  @Slf4j
  @RequiredArgsConstructor
  public static class SparkPipelineRunner implements ApplicationRunner {

    private final SparkUDFExecutor sparkUDFExecutor;
    private final SparkBucketizeExecutor sparkBucketizeExecutor;

    @Override
    public void run(final ApplicationArguments args) {
//      this.sparkUDFExecutor.execute();
      this.sparkBucketizeExecutor.execute();
    }
  }
}
