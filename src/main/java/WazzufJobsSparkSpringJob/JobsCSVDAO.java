package WazzufJobsSparkSpringJob;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JobsCSVDAO implements JobsDAO {

    public JobsCSVDAO() {
    }
    String path = "src/main/resources/Wuzzuf_Jobs.csv";
    public static void main(String[] args) {
        JobsCSVDAO job = new JobsCSVDAO ();
        try {
            job.readJobsFromCSV (job.path);
        } catch (IOException e) {
            e.printStackTrace ();
        }
    }

    @Override
    public List<WazzufJobs> readJobsFromCSV(String path) throws IOException {
        List<WazzufJobs> wjobs=new ArrayList<>();
        Path csvFile = Paths.get (path);
        try (BufferedReader reader = Files.newBufferedReader (csvFile, StandardCharsets.UTF_8)) {
            CSVFormat csv = CSVFormat.RFC4180.withHeader ();
            try (CSVParser parser = csv.parse (reader)) {
                Iterator<CSVRecord> it = parser.iterator ();
                it.forEachRemaining (rec -> {
                    String title = rec.get ("Title");
                    String company = rec.get ("Company");
                    String location = rec.get ("Location");
                    String type = rec.get ("Type");
                    String level = rec.get ("Level");
                    String yearsExp = rec.get ("YearsExp");
                    String country = rec.get ("Country");
                    String skills = rec.get ("Skills");

                    WazzufJobs wj = new WazzufJobs(title, company, location,type,level,yearsExp,country,skills);
                    wjobs.add (wj);
                }  );
            }
        }
        //wjobs.stream ().forEach (System.out::println);
        return wjobs;
    }

    public WazzufJobs createJob(String[] metadata)
    {
        String title=metadata[0];
        String company=metadata[1];
        String location=metadata[2];
        String type=metadata[3];
        String level=metadata[4];
        String yearsExp=metadata[5];
        String country=metadata[6];
        String skills=metadata[7];
        return new WazzufJobs(title,company,location,type,level,yearsExp,country,skills);

    }
}
