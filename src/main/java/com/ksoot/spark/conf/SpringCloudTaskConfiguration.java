package com.ksoot.spark.conf;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.cloud.task.configuration.DefaultTaskConfigurer;
import org.springframework.cloud.task.configuration.TaskProperties;
import org.springframework.cloud.task.repository.TaskExecution;
import org.springframework.context.MessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@ConditionalOnClass(TaskExecution.class)
class SpringCloudTaskConfiguration {

  @Bean
  @Primary
  // To make Spring cloud task to not use any database but in memory only.
  DefaultTaskConfigurer taskConfigurer() {
    return new DefaultTaskConfigurer(TaskProperties.DEFAULT_TABLE_PREFIX);
  }

  @Bean
  JobExecutionListener jobExecutionListener(final MessageSource messageSource) {
    return new JobExecutionListener(messageSource);
  }
}
