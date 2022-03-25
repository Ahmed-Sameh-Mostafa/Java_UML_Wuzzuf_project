package WazzufJobsSparkSpringJob;

import java.io.Serializable;
import java.math.BigInteger;

public class MagicTrick implements Serializable {
    private String Voila;

    public MagicTrick(String voila) {
        Voila = voila;
    }

    public MagicTrick() {
    }

    public String getVoila() {
        return Voila;
    }

    public void setVoila(String voila) {
        if (voila == null)
        {
            Voila = "null";
        }
        else {
            Voila = voila;
        }
    }

    @Override
    public String toString() {
        return Voila ;
    }
}
