package WazzufJobsSparkSpringJob;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.ui.Model;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Map;

import java.util.*;
import java.util.stream.Collectors;


@Component
public class Services {

    @Autowired
    Functions func;

    @Autowired
    private SparkSession sparkSession;
    /**1. Read Data, Convert to spark Dataset and Display it
     * 2. Display Structure and Summary of Dataset
     * 3. Clean the Dataset**/

    public String ReadDisplayClean(Model model) throws IOException
    {
        //Html string variable to be changed later
        String html = "Section_I";

        //Spark dataset to be displayed
        Dataset<WazzufJobs> OriginalData = func.dataRead();

        //Summary as dataset to be displayed
        Dataset <Row> OriginalDataSummary = func.dataDescribe(OriginalData);

        //Schema as StructType to be dispalyed
        StructType OriginalSchema = func.dataStructure(OriginalData);

        //Cleaned dataset to be displayed
        Dataset<WazzufJobs> CleanedData = func.dataClean();
        //Cleaned dataset Summary to be displayed
        Dataset<Row> CleanedDataSummary = func.dataDescribe(CleanedData);

        /***Write your Model Code here***/

        //Displaying Data before Cleaning
        model.addAttribute("title", View.abraCadabra(OriginalData,"title"));
        model.addAttribute("company", View.abraCadabra(OriginalData,"company"));
        model.addAttribute("location", View.abraCadabra(OriginalData,"location"));
        model.addAttribute("type", View.abraCadabra(OriginalData,"type"));
        model.addAttribute("level", View.abraCadabra(OriginalData,"level"));
        model.addAttribute("yearsExp", View.abraCadabra(OriginalData,"yearsExp"));
        model.addAttribute("country", View.abraCadabra(OriginalData,"country"));
        model.addAttribute("skills", View.abraCadabra(OriginalData,"skills"));

        //Displaying Original Data Summary
        model.addAttribute("summary", View.abraCadabra(OriginalDataSummary,"summary"));
        model.addAttribute("title_S", View.abraCadabra(OriginalDataSummary,"title"));
        model.addAttribute("company_S", View.abraCadabra(OriginalDataSummary,"company"));
        model.addAttribute("location_S", View.abraCadabra(OriginalDataSummary,"location"));
        model.addAttribute("type_S", View.abraCadabra(OriginalDataSummary,"type"));
        model.addAttribute("level_S", View.abraCadabra(OriginalDataSummary,"level"));
        model.addAttribute("yearsExp_S", View.abraCadabra(OriginalDataSummary,"yearsExp"));
        model.addAttribute("country_S", View.abraCadabra(OriginalDataSummary,"country"));
        model.addAttribute("skills_S", View.abraCadabra(OriginalDataSummary,"skills"));

        //Displaying schema
        List<String> schema_name = Arrays.stream(OriginalSchema.fields()).collect(Collectors.toList()).stream().map(x -> x.name()).collect(Collectors.toList());
        List<String> schema_type = Arrays.stream(OriginalSchema.fields()).collect(Collectors.toList()).stream().map(x -> x.dataType().toString()).collect(Collectors.toList());
        List<Boolean> temp = Arrays.stream(OriginalSchema.fields()).collect(Collectors.toList()).stream().map(x -> x.nullable()).collect(Collectors.toList());
        List<String> schema_nullable = temp.stream().map(x -> x.toString()).collect(Collectors.toList());
        List<String> schema_meta = Arrays.stream(OriginalSchema.fields()).collect(Collectors.toList()).stream().map(x -> x.metadata().toString()).collect(Collectors.toList());

        MagicWrapper schema_name_M = new MagicWrapper(schema_name.stream().map(x -> new MagicTrick(x)).collect(Collectors.toList()));
        MagicWrapper schema_type_M = new MagicWrapper(schema_type.stream().map(x -> new MagicTrick(x)).collect(Collectors.toList()));
        MagicWrapper schema_nullable_M = new MagicWrapper(schema_nullable.stream().map(x -> new MagicTrick(x)).collect(Collectors.toList()));
        MagicWrapper schema_meta_M = new MagicWrapper(schema_meta.stream().map(x -> new MagicTrick(x)).collect(Collectors.toList()));

        model.addAttribute("schema_name",schema_name_M);
        model.addAttribute("schema_type",schema_type_M);
        model.addAttribute("schema_nullable",schema_nullable_M);
        model.addAttribute("schema_meta",schema_meta_M);

        //Displaying Cleaned Data
        model.addAttribute("title_C", View.abraCadabra(CleanedData,"title"));
        model.addAttribute("company_C", View.abraCadabra(CleanedData,"company"));
        model.addAttribute("location_C", View.abraCadabra(CleanedData,"location"));
        model.addAttribute("type_C", View.abraCadabra(CleanedData,"type"));
        model.addAttribute("level_C", View.abraCadabra(CleanedData,"level"));
        model.addAttribute("yearsExp_C", View.abraCadabra(CleanedData,"yearsExp"));
        model.addAttribute("country_C", View.abraCadabra(CleanedData,"country"));
        model.addAttribute("skills_C", View.abraCadabra(CleanedData,"skills"));

        //Displaying Cleaned Data Summary
        model.addAttribute("summary_C", View.abraCadabra(CleanedDataSummary,"summary"));
        model.addAttribute("title_S_C", View.abraCadabra(CleanedDataSummary,"title"));
        model.addAttribute("company_S_C", View.abraCadabra(CleanedDataSummary,"company"));
        model.addAttribute("location_S_C", View.abraCadabra(CleanedDataSummary,"location"));
        model.addAttribute("type_S_C", View.abraCadabra(CleanedDataSummary,"type"));
        model.addAttribute("level_S_C", View.abraCadabra(CleanedDataSummary,"level"));
        model.addAttribute("yearsExp_S_C", View.abraCadabra(CleanedDataSummary,"yearsExp"));
        model.addAttribute("country_S_C", View.abraCadabra(CleanedDataSummary,"country"));
        model.addAttribute("skills_S_C", View.abraCadabra(CleanedDataSummary,"skills"));

        return html;
    }




    /**4. Count the jobs for each company and display it as dataset (+ Most demeanding jobs)
     * 5. Show dataset in PieChart
     * **/

    public String CompanyJobs (Model model) throws IOException
    {
        //Html string variable to be changed
        String html = "Section_II";

        //Calling cleaned dataset to be used with CountCompanyJobs function
        Dataset<WazzufJobs> CleanedDataset = func.dataClean();

        //Companies-Jobs Counts as dataset to be displayed
        Dataset<Row> CompaniesJobs = func.CountComapnyJobs(CleanedDataset);
        CompaniesJobs.createOrReplaceTempView ("jobsCount");
        Dataset<Row> jobsCountString = sparkSession.sql("SELECT CAST(Company as string) Company, CAST(count as string) count FROM jobsCount");

        //Most Demanding Companies as Dataset to be displayed
        Dataset<Row> MostCompaniesJobs = jobsCountString.limit(20);

        //Path for Piechart as image to be displayed
        String JobsPiePath = func.PieChart(CompaniesJobs, "CompJobPie.jpg");


        /***Write your Model Code here***/

        //Displaying Data before Cleaning
        model.addAttribute("company", View.abraCadabra(jobsCountString,"Company"));
        model.addAttribute("count", View.abraCadabra(jobsCountString,"count"));

        model.addAttribute("company_M", View.abraCadabra(MostCompaniesJobs,"Company"));
        model.addAttribute("count_M", View.abraCadabra(MostCompaniesJobs,"count"));



        return html;
    }


    /**6. Find out the most Popular Jobs and display it as dataset
     * 7. Show dataset in BarChart**/

    public String PopularJobs (Model model) throws IOException
    {
        //Html string variable to be changed
        String html = "Section_III";

        //Calling cleaned dataset to be used with mostPopularJobs function
        Dataset<WazzufJobs> CleanedDataset = func.dataClean();

        //All the Jobs dataset (not for displaying)
        Dataset<Row> PopularJobs = func.mostPopularJobs(CleanedDataset);
        PopularJobs.createOrReplaceTempView ("popularJobs");
        Dataset<Row> popularJobString = sparkSession.sql("SELECT CAST(Title as string) Title, CAST (count as string) count FROM popularJobs ");

        //Most Popular jobs as dataset to be dispalyed
        Dataset<Row> MostPopularJobs = popularJobString.limit(20);

        //Path for Barchart as image to be displayed
        String JobsBarPath = func.BarChart(PopularJobs, "PopJobsBar.jpg");


        /***Write your Model Code here***/
        model.addAttribute("title", View.abraCadabra(MostPopularJobs,"title"));
        model.addAttribute("count", View.abraCadabra(MostPopularJobs,"count"));

        return html;
    }


    /**8. Find out most Popular Areas and dispaly it as dataset
     * 9. Show dataset in BarChart**/
    public String PopularAreas (Model model) throws IOException
    {
        //Html string variable to be changed
        String html = "Section_IV";

        //Calling cleaned dataset to be used with mostPopularAreas function
        Dataset<WazzufJobs> CleanedDataset = func.dataClean();

        //All the areas as dataset (not for displaying)
        Dataset<Row> PopularAreas = func.mostPopularAreas(CleanedDataset);
        PopularAreas.createOrReplaceTempView ("popularArea");
        Dataset<Row> popularAreaString = sparkSession.sql("SELECT CAST(Location as string) Location, CAST (count as string) count FROM popularArea ");

        //Most Popular Areas as dataset to be displayed
        Dataset<Row> MostPopularAreas = popularAreaString.limit(20);

        //Path for Barchart as image to be displayed
        String AreasBarPath = func.BarChart(PopularAreas, "PopAreas.jpg");


        /***Write your Model Code here***/
        model.addAttribute("location", View.abraCadabra(MostPopularAreas,"location"));
        model.addAttribute("count", View.abraCadabra(MostPopularAreas,"count"));


        return html;

    }


    /**10. Print skills one by one as dataset
     *  10.1. How many each repeated as dataset
     *  10.2 Find out the most important skills**/

    public String SkillsFlatten (Model model) throws IOException
    {
        //Html string variable to be changed
        String html = "Section_V";

        //Calling cleaned dataset to be used with Skills function
        Dataset<WazzufJobs> CleanedDataset = func.dataClean();

        //Skills as dataset to be displayed
        Dataset<Row> SkillsFlattenData = func.Skills(CleanedDataset);

        //Skills Count as Map data to be displayed
        Set<Map.Entry<String, String>> SkillsCountData = func.SkillsCount(CleanedDataset);
        List<String> skills_list=new ArrayList<>();
        List<String> skills_count=new ArrayList<>();
        int i=0;
        for(Map.Entry<String, String> mapping : SkillsCountData){
            skills_list.add(i,mapping.getKey() );
            skills_count.add(i,mapping.getValue());
            i=i+1;
        }
        List<MagicTrick> skills_list_M = skills_list.stream().map(x -> new MagicTrick(x)).collect(Collectors.toList());
        List<MagicTrick> skills_count_M = skills_count.stream().map(x -> new MagicTrick(x)).collect(Collectors.toList());

        /***Write your Model Code here***/
        model.addAttribute("skills", View.abraCadabra(SkillsFlattenData,"skills"));

        model.addAttribute("skills_M", new MagicWrapper(skills_list_M));
        model.addAttribute("count_M", new MagicWrapper(skills_count_M));

        return html;
    }



    /**11. Factorize YearsExp feature and convert it to numbers in new col
     * **/
    public String YearsExpFactorize(Model model) throws IOException
    {
        //Html string variable to be changed
        String html = "Section_VI";

        //Calling cleaned dataset to be used with FactorizeYrsExp function
        Dataset<WazzufJobs> CleanedDataset = func.dataClean();

        //Dataset with indexed factorized column to be displayed
        Dataset<Row> FactorizedData = func.FactorizeYrsExp(CleanedDataset);


        /***Write your Model Code here***/
        model.addAttribute("title", View.abraCadabra(FactorizedData,"title"));
        model.addAttribute("company", View.abraCadabra(FactorizedData,"company"));
        model.addAttribute("location", View.abraCadabra(FactorizedData,"location"));
        model.addAttribute("type", View.abraCadabra(FactorizedData,"type"));
        model.addAttribute("level", View.abraCadabra(FactorizedData,"level"));
        model.addAttribute("yearsExp", View.abraCadabra(FactorizedData,"yearsExp"));
        model.addAttribute("country", View.abraCadabra(FactorizedData,"country"));
        model.addAttribute("skills", View.abraCadabra(FactorizedData,"skills"));
        model.addAttribute("YearsExpCategory", View.abraCadabra(FactorizedData,"YearsExpCategory"));



        return html;
    }


    /**12. Apply K-means for Job titles and Companies**/

    public String kMeans (Model model) throws IOException
    {
        //Html string variable to be changed
        String html = "Section_VII";

        //Calling cleaned dataset to be used with Kmeans function
        Dataset<WazzufJobs> CleanedDataset = func.dataClean();

        //Kmeans as dataset to be displayed
        Dataset<Row> kmeanData = func.kMeans(CleanedDataset);

        //change kmeans to long
        Dataset<Row> kmeanData_Long =kmeanData;
        kmeanData_Long= kmeanData_Long.withColumn("TitleIndex",kmeanData.col("TitleIndex").cast("Long"));
        kmeanData_Long= kmeanData_Long.withColumn("CompanyIndex",kmeanData.col("CompanyIndex").cast("Long"));
        kmeanData_Long= kmeanData_Long.withColumn("prediction",kmeanData.col("prediction").cast("Long"));
        //Kmeans XYChart as image to be displayed
        String KmeanXYChartPath = func.XYchart_Kmeans(kmeanData_Long);


        /***Write your Model Code here***/

        return html;
    }
}
