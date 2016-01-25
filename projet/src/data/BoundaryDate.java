package data;

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
            case "BEGINNING":
                return BEGINNING;
            case "END":
                return END;
            default:
                throw new InvalidParameterException("String " + string + " does not match any enum field");
        }
    }

    public boolean isBeginning(){
        return this.equals(BEGINNING);
    }
    public boolean isEnd(){
        return this.equals(END);
    }

}
