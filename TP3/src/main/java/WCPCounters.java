import java.util.NoSuchElementException;

/**
 * Created by rchabot on 18/11/15.
 */
public enum WCPCounters {
    NB_POP, TOT_POP, NB_CITIES, NB_INVALID_CITIES;

    @Override
    public String toString() {
        switch (this) {
            case NB_POP:
                return "nb_pop";
            case TOT_POP:
                return "tot_pop";
            case NB_CITIES:
                return "nb_cities";
            case NB_INVALID_CITIES:
                return "nb_invalid_cities";
            default:
                throw new NoSuchElementException();
        }
    }
}
