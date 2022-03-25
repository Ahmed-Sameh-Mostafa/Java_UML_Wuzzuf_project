package WazzufJobsSparkSpringJob;

import java.io.Serializable;

public class WazzufJobs implements Serializable {
    String Title;
    String Company;
    String Location;
    String Type;
    String Level;
    String YearsExp;
    String Country;
    String Skills;


    public WazzufJobs() {
    }


    public  WazzufJobs(String title, String company, String location, String type, String level,String yearsExp, String country,String skills)
    {
       this.Title=title;
       this.Company=company;
       this.Location=location;
       this.Type=type;
       this.Level=level;
       this.YearsExp=yearsExp;
       this.Country=country;
       this.Skills=skills;
    }

    public String getTitle(){ return  Title;}
    public void  setTitle(String title){this.Title=title;}

    public String getCompany(){return Company;}
    public void  setCompany(String company){this.Company=company;}

    public String getLocation(){return Location;}
    public void  setLocation(String location){this.Location=location;}

    public String getType(){return this.Type;}
    public void  setType(String type){this.Type=type;}

    public String getLevel(){return Level;}
    public void setLevel(String level){this.Level=level;}

    public String getYearsExp(){return YearsExp;}
    public void  setYearsExp(String yearsExp){this.YearsExp= yearsExp;}

    public String getCountry(){return Country;}
    public void setCountry(String country){this.Country=country;}

    public String getSkills(){return Skills;}
    public void setSkills(String skills){this.Skills=skills;}


    @Override
    public String toString() {
        return "job{" +
                "title=" + Title +
                ", company=" + Company +
                ", location='" + Location + '\'' +
                ", type='" + Type + '\'' +
                ", level=" + Level +
                ", yearsExp=" + YearsExp +
                ", country=" + Country +
                ", skills='" + Skills + '\'' +
                '}'+"\n";
    }

}

