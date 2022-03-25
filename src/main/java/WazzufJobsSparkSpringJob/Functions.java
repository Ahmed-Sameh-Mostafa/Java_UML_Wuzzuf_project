package WazzufJobsSparkSpringJob;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import org.knowm.xchart.style.markers.SeriesMarkers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.stream.Collectors;
import java.util.*;
import static org.apache.spark.sql.functions.desc;

@Component
public class Functions {
    @Autowired
    private SparkSession sparkSession;
    @Autowired
    String path ;


    public Dataset<WazzufJobs> dataRead() throws IOException
    {
        JobsCSVDAO jobs_obj = new JobsCSVDAO();
        List<WazzufJobs> wjobs = jobs_obj.readJobsFromCSV(path);
        Encoder<WazzufJobs> encoder = Encoders.bean(WazzufJobs.class);
        Dataset<WazzufJobs> dataset = sparkSession.createDataset(wjobs, encoder);
        return dataset;
    }

    public Dataset<Row> dataDescribe(Dataset dataset) throws IOException
    {
        //      Dataset<WazzufJobs> dataset = dataRead();
        Dataset<Row> Summary=dataset.describe().limit(1);
        return Summary;
    }
    public StructType dataStructure (Dataset dataset) throws IOException
    {
        //Dataset<WazzufJobs> dataset = dataRead();
        StructType schema = dataset.schema();
        return schema;

    }
    public Dataset<WazzufJobs> dataClean() throws IOException {
        Dataset<WazzufJobs> dataset = dataRead();
        Dataset<WazzufJobs> Filtered_Dataset= dataset.filter(dataset.col("yearsExp").notEqual("null Yrs of Exp")).dropDuplicates();
        List<String> skills_col= Filtered_Dataset.select("Skills").as(Encoders.STRING()).collectAsList();
        List<String> years_col= Filtered_Dataset.select("YearsExp").as(Encoders.STRING()).collectAsList();
        for (int i = 0; i < skills_col.size(); i++)
        {
            if (skills_col.get(i).contains(years_col.get(i)+", "))
            {
                skills_col.set(i,skills_col.get(i).replace(years_col.get(i)+", ", ""));
            }
        }
        List<WazzufJobs> dataset_list = Filtered_Dataset.collectAsList();
        for (int i=0; i<dataset_list.size(); i++)
        {
            dataset_list.get(i).setSkills(skills_col.get(i));
        }
        //  System.out.println(dataset_list);
        Encoder<WazzufJobs> encoder = Encoders.bean(WazzufJobs.class);

        Dataset<WazzufJobs> Final_data = sparkSession.createDataset(dataset_list,encoder);
        return Final_data;
    }

    public Dataset<Row> CountComapnyJobs(Dataset dataset) throws IOException {

        Dataset<Row> jobstitle = dataset.select("Company", "Title");
        Dataset<Row> jobsCount = jobstitle.groupBy("Company").count().sort(desc("count"));
        return jobsCount;
    }

    public Dataset<Row> mostPopularJobs(Dataset dataset) throws IOException {

        Dataset<Row> popularJobs = dataset.groupBy("Title").count().sort(desc("count"));

        return popularJobs;
    }



    public Dataset<Row> mostPopularAreas(Dataset dataset) throws IOException {
        Dataset<Row> popularArea = dataset.groupBy("Location").count().sort(desc("count"));

        return popularArea;
    }


    public Dataset<Row> Skills (Dataset dataset)
    {
        List<String> skills_col = dataset.select("Skills").as(Encoders.STRING()).collectAsList();
        List<String> skills_col_flat= skills_col.stream().map(string -> string.split(","))
                .flatMap(Arrays::stream).collect(Collectors.toList());
        Dataset<Row> Skills_Dataset=sparkSession.createDataset(skills_col_flat,Encoders.STRING()).toDF("Skills");

        return Skills_Dataset;
    }

    public Set<Map.Entry<String, String>> SkillsCount(Dataset dataset) throws IOException {

        List<String> skills_col = dataset.select("Skills").as(Encoders.STRING()).collectAsList();
        List<String> skills_col_flat= skills_col.stream().map(string -> string.split(","))
                .flatMap(Arrays::stream).collect(Collectors.toList());
        Map<String, Long> resultMap = new HashMap<>();
        skills_col_flat.forEach(e -> resultMap.put(e, resultMap.getOrDefault(e, 0L) + 1L));

        Set<Entry<String, Long>> entries = resultMap.entrySet();
        Comparator<Entry<String, Long>> valueComparator
                = new Comparator<Entry<String,Long>>() {
            @Override
            public int compare(Entry<String, Long> e1, Entry<String, Long> e2) {
                Long v1 = e1.getValue();
                Long v2 = e2.getValue();
                return v2.compareTo(v1);
            }
        };
        // Sort method needs a List, so let's first convert Set to List in Java
        List<Entry<String, Long>> listOfEntries
                = new ArrayList<Entry<String, Long>>(entries);

        // sorting HashMap by values using comparator
        Collections.sort(listOfEntries, valueComparator);
        LinkedHashMap<String, String> sortedByValue
                = new LinkedHashMap<String, String>(listOfEntries.size());

        // copying entries from List to Map
        for(Entry<String, Long> entry : listOfEntries){
            sortedByValue.put(entry.getKey(), entry.getValue().toString());
        }
        Set<Entry<String, String>> entrySetSortedByValue = sortedByValue.entrySet();
        return entrySetSortedByValue;

    }

    public Dataset<Row> FactorizeYrsExp(Dataset dataset) throws IOException {

        StringIndexerModel indexer = new StringIndexer()
                .setInputCol("yearsExp")
                .setOutputCol("YearsExpCategory")
                .fit(dataset);
        Dataset<Row> data_indexed = indexer.transform(dataset);
        data_indexed.createOrReplaceTempView ("data_indexed");
        Dataset<Row> data_indexedString = sparkSession.sql("SELECT CAST(Company as string) Company, CAST (Country as string) Country,"
                + "CAST (Level as string) Level, CAST(Location as string) Location,"
                + "CAST (Skills as string) Skills, CAST (Title as string) Title,"
                + "CAST (Type as string) Type, CAST (YearsExp as string) YearsExp,"
                + "CAST (YearsExpCategory as string) YearsExpCategory FROM data_indexed ");

        return data_indexedString;
    }


    public Dataset<Row> kMeans (Dataset dataset) {
        Dataset Kmeans_data=dataset.select("Title","Company");
        Kmeans_data.show(4);

        StringIndexer indexer = new StringIndexer()
                .setInputCol("Title")
                .setOutputCol("TitleIndex");
        Dataset<Row> indexed = indexer.fit(Kmeans_data).transform(Kmeans_data);
        //indexed.show(4);

        StringIndexer indexer1 = new StringIndexer()
                .setInputCol("Company")
                .setOutputCol("CompanyIndex");
        Dataset<Row> indexed1 = indexer1.fit(indexed).transform(indexed);
        //indexed1.show(4);


        Kmeans_data=indexed1.drop("Title","Company");
        //Kmeans_data.show(4);

        //============================================================================================================
        String inputColumns[] = {"TitleIndex","CompanyIndex"};
        //Create the Vector Assembler That will contain the feature columns
        VectorAssembler vectorAssembler = new VectorAssembler ();
        vectorAssembler.setInputCols (inputColumns);
        vectorAssembler.setOutputCol ("features");
        Dataset<Row> KmeansTransform = vectorAssembler.transform (Kmeans_data);
        //============================================================================================================

        // Trains a k-means model.
        KMeans kmean = new KMeans().setK(3).setSeed(1L);
        KMeansModel model = kmean.fit(KmeansTransform);

        // Make predictions
        Dataset<Row> predictions = model.transform(KmeansTransform);
        //predictions.show(4);


        org.apache.spark.ml.linalg.Vector[] centers = model.clusterCenters();
//
//        System.out.println("Cluster Centers: ");
//        for (Vector center: centers) {
//            System.out.println(center);
//        }

        Dataset<Row> kmeans=predictions.drop("features");
        // kmeans.show (5);
        //============================================================================================================
        kmeans= kmeans.withColumn("TitleIndex",kmeans.col("TitleIndex").cast("String"));
        kmeans= kmeans.withColumn("CompanyIndex",kmeans.col("CompanyIndex").cast("String"));
        kmeans= kmeans.withColumn("prediction",kmeans.col("prediction").cast("String"));
        return kmeans;

    }
    public String XYchart_Kmeans(Dataset<Row> kmeans) throws IOException {

        Dataset<Row> class0= kmeans.filter(kmeans.col("prediction").equalTo(0));

        List<Long> TITLE0 = class0.select("TitleIndex").collectAsList().stream()
                .map(row -> row.getLong(0)).collect(Collectors.toList());
        List<Long> Company0 = class0.select("CompanyIndex").collectAsList().stream()
                .map(row -> row.getLong(0)).collect(Collectors.toList());

        Dataset<Row> class1= kmeans.filter(kmeans.col("prediction").equalTo(1));
        List<Long> TITLE1 = class1.select("TitleIndex").collectAsList().stream()
                .map(row -> row.getLong(0)).collect(Collectors.toList());
        List<Long> Company1 = class1.select("CompanyIndex").collectAsList().stream()
                .map(row -> row.getLong(0)).collect(Collectors.toList());

        Dataset<Row> class2= kmeans.filter(kmeans.col("prediction").equalTo(2));
        List<Long> TITLE2 = class2.select("TitleIndex").collectAsList().stream()
                .map(row -> row.getLong(0)).collect(Collectors.toList());
        List<Long> Company2 = class2.select("CompanyIndex").collectAsList().stream()
                .map(row -> row.getLong(0)).collect(Collectors.toList());

        String path= XYChart (TITLE0, Company0, TITLE1, Company1,TITLE2,Company2, "KmeanXYChart.jpg");

        return path;


    }
    public String XYChart(List<Long> X1,List<Long> Y1,List<Long> X2,List<Long> Y2,List<Long> X3,List<Long> Y3, String Chartname) throws IOException {

// Create Chart
        XYChart chart = new XYChartBuilder().width(600).height(500).title("K-MEANS CLUSTERS").xAxisTitle("TITLE").yAxisTitle("COMPANY").build();

// Customize Chart
        chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideSW);
        chart.getStyler().setMarkerSize(16);

// Series
        chart.addSeries("cluster 0", X1, Y1);
        XYSeries series = chart.addSeries("cluster 1", X2, Y2);
        XYSeries series1 = chart.addSeries("cluster 2", X3, Y3);
        series.setMarker(SeriesMarkers.DIAMOND);
        series1.setMarker(SeriesMarkers.PLUS);
        String ChartPath = "src/main/resources/static/images/"+Chartname;
        BitmapEncoder.saveJPGWithQuality(chart, ChartPath, 0.95f);
        return ChartPath;

    }
    public String PieChart(Dataset<Row> dataset, String Chartname) throws IOException {
        List<String> col_1 = dataset.select(dataset.columns()[0]).as(Encoders.STRING()).collectAsList().subList(0, 20);
        List<Long> col_2 = dataset.select(dataset.columns()[1]).collectAsList().stream().map(row -> row.getLong(0)).collect(Collectors.toList()).subList(0, 20);

        PieChart piechart = new PieChartBuilder().width(800).height(600).build();
        //chart.getStyler().setAnnotationType(PieStyler.AnnotationType.LabelAndPercentage);
        for (int i=1 ;i<5;i++){
        piechart.addSeries(col_1.get(i),  col_2.get(i));
        }
//        new SwingWrapper(piechart).displayChart();

        String ChartPath = "src/main/resources/static/images/" + Chartname;
        BitmapEncoder.saveJPGWithQuality(piechart, ChartPath, 0.95f);
        return ChartPath;
    }
    public String BarChart(Dataset<Row> dataset, String Chartname) throws IOException {
        List<String> col_1 = dataset.select(dataset.columns()[0]).as(Encoders.STRING()).collectAsList().subList(0, 20);
        List<Long> col_2 = dataset.select(dataset.columns()[1]).collectAsList().stream().map(row -> row.getLong(0)).collect(Collectors.toList()).subList(0, 20);
        //System.out.println(col_jobs);
        //System.out.println(col_count);

        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).build();//.title("Score Histogram").xAxisTitle("Score").yAxisTitle("Number").build();
        // Customize Chart
        // chart.getStyler().setChartTitleVisible(true);
        chart.getStyler().setXAxisLabelRotation(90);
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        // Series
        chart.addSeries("Jobs Count", col_1, col_2);
        // Show it
        String ChartPath = "src/main/resources/static/images/" + Chartname;
        BitmapEncoder.saveJPGWithQuality(chart, ChartPath, 0.95f);
        return ChartPath;
    }
}