package WazzufJobsSparkSpringJob;

import java.util.List;

public class MagicWrapper {
    private List<MagicTrick> data;

    public MagicWrapper() {
    }

    public MagicWrapper(List<MagicTrick> data) {
        this.data = data;
    }

    public List<MagicTrick> getData() {
        return data;
    }

    public void setData(List<MagicTrick> data) {
        this.data = data;
    }
}
