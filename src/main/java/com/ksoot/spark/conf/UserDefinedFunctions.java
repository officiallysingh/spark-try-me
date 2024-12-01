package com.ksoot.spark.conf;

import static org.apache.spark.sql.functions.callUDF;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.functions;

public class UserDefinedFunctions {

  public static final String EXPLODE_DATE_SEQ = "explodeDateSeq";

  static UDF2<LocalDate, LocalDate, List<LocalDate>> explodeDateSeq =
      (start, end) -> {
        long numOfDaysBetween = ChronoUnit.DAYS.between(start, end) + 1;
        return Stream.iterate(start, date -> date.plusDays(1)).limit(numOfDaysBetween).toList();
      };

  public static Column explodeDateSeq(final Column startCol, final Column endCol) {
    return functions.explode(callUDF(UserDefinedFunctions.EXPLODE_DATE_SEQ, startCol, endCol));
  }
}
