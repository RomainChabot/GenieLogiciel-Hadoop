import java.security.InvalidParameterException;

public enum BoundaryDate {
    BEGINNING,
    END;

    @Override
    public String toString() {
        return super.toString();
    }
    public static BoundaryDate strToEnum(String string){
        switch (string){
            case "beginning":
                return BEGINNING;
            case "end":
                return END;
            default:
                throw new InvalidParameterException("String " + string + " does not match any enum field");
        }
    }

    public boolean isBeginning(BoundaryDate bDate){
        return bDate.equals(BEGINNING);
    }
    public boolean isEnd(BoundaryDate bDate){
        return bDate.equals(END);
    }

}
