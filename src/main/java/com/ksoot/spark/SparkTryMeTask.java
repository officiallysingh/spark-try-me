package com.ksoot.spark;

import jakarta.annotation.PostConstruct;
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
  public ApplicationRunner applicationRunner(final SparkTaskExecutor sparkTaskExecutor) {
    return new SparkPipelineRunner(sparkTaskExecutor);
  }

  @Slf4j
  public static class SparkPipelineRunner implements ApplicationRunner {

    private final SparkTaskExecutor sparkTaskExecutor;

    public SparkPipelineRunner(final SparkTaskExecutor sparkTaskExecutor) {
      this.sparkTaskExecutor = sparkTaskExecutor;
    }

    @Override
    public void run(final ApplicationArguments args) {
      this.sparkTaskExecutor.execute();
    }
  }
}
