public class DataStreamCalcRule$23 implements org.apache.flink.api.common.functions.FlatMapFunction {


  transient org.apache.flink.api.table.Row out = new org.apache.flink.api.table.Row(3);

  public DataStreamCalcRule$23() throws Exception {


  }

  @Override
  public void flatMap(Object _in1, org.apache.flink.util.Collector c) throws Exception {
    org.apache.flink.api.table.Row in1 = (org.apache.flink.api.table.Row) _in1;

    java.lang.Integer tmp$16 = (java.lang.Integer) in1.productElement(2);
    boolean isNull$18 = tmp$16 == null;
    int result$17;
    if (isNull$18) {
      result$17 = -1;
    } else {
      result$17 = tmp$16;
    }


    java.lang.Long tmp$10 = (java.lang.Long) in1.productElement(0);
    boolean isNull$12 = tmp$10 == null;
    long result$11;
    if (isNull$12) {
      result$11 = -1;
    } else {
      result$11 = tmp$10;
    }


    java.lang.String result$14 = (java.lang.String) in1.productElement(1);
    boolean isNull$15 = (java.lang.String) in1.productElement(1) == null;


    int result$19 = 2;
    boolean isNull$20 = false;

    boolean isNull$22 = isNull$18 || isNull$20;
    boolean result$21;
    if (isNull$22) {
      result$21 = false;
    } else {
      result$21 = result$17 > result$19;
    }

    if (result$21) {


      if (isNull$12) {
        out.setField(0, null);
      } else {
        out.setField(0, result$11);
      }


      if (isNull$15) {
        out.setField(1, null);
      } else {
        out.setField(1, result$14);
      }


      if (isNull$18) {
        out.setField(2, null);
      } else {
        out.setField(2, result$17);
      }

      c.collect(out);
    }

  }
}