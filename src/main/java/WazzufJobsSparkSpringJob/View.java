package WazzufJobsSparkSpringJob;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.List;

public class View {

    public static MagicWrapper abraCadabra(Dataset data, String colName)
    {
        Dataset<Row> dataset = data.select(colName).withColumnRenamed(colName,"voila");
        Encoder<MagicTrick> encoder = Encoders.bean(MagicTrick.class);
        List<MagicTrick> dataList = dataset.as(encoder).collectAsList();
        MagicWrapper test = new MagicWrapper(dataList);

        return test;
    }
}
