package com.ksoot.spark.conf;

import com.ksoot.spark.springframework.boot.autoconfigure.SparkSessionCustomizer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;

@Component
class SparkSessionUDFCustomizer implements SparkSessionCustomizer, Ordered {

  @Override
  public void customize(final SparkSession sparkSession) {
    sparkSession
        .udf()
        .register(
            UserDefinedFunctions.EXPLODE_DATE_SEQ,
            UserDefinedFunctions.explodeDateSeq,
            DataTypes.createArrayType(DataTypes.DateType));
  }

  @Override
  public int getOrder() {
    return Ordered.HIGHEST_PRECEDENCE;
  }
}
