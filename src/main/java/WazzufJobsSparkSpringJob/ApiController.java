package WazzufJobsSparkSpringJob;

import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

/**
 * Created by Owner on 2017. 03. 29..
 */
@Controller
public class ApiController {
    @Autowired
    Functions func;
    @Value("${spring.application.name}")
    String appName;
    @Autowired
    String path ;
    @Autowired
    Services services;

    @GetMapping ("/Section_I")
    public String section_I(Model model) throws IOException {
        return services.ReadDisplayClean(model);
    }

    @GetMapping ("/Section_II")
    public String section_II(Model model) throws IOException {
        return services.CompanyJobs(model);
    }

    @GetMapping ("/Section_III")
    public String section_III(Model model) throws IOException {
        return services.PopularJobs(model);
    }

    @GetMapping ("/Section_IV")
    public String section_IV(Model model) throws IOException {
        return services.PopularAreas(model);
    }

    @GetMapping ("/Section_V")
    public String section_V(Model model) throws IOException {
        return services.SkillsFlatten(model);
    }

    @GetMapping ("/Section_VI")
    public String section_VI(Model model) throws IOException {
        return services.YearsExpFactorize(model);
    }

    @GetMapping ("/Section_VII")
    public String section_VII(Model model) throws IOException {
        return services.kMeans(model);
    }

    @GetMapping ("/")
    public String section_0(Model model) throws IOException {
        return "GUI";
    }

    @RequestMapping("/helloWorld")
    public @ResponseBody String greeting() {
        return "Hello, World";
    }

}
