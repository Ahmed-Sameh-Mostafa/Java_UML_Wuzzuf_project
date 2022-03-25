package WazzufJobsSparkSpringJob;

import java.io.IOException;
import java.util.List;

public interface JobsDAO  {

    public List<WazzufJobs> readJobsFromCSV(String filename) throws IOException;
}
