package example;

public class KeyMappingObject {
    private String CARD_NUMBER;
    private String KEY_TIME;
    private Long START_TIME;
    private Long END_TIME;

    public String getCARD_NUMBER() {
        return CARD_NUMBER;
    }

    public void setCARD_NUMBER(String CARD_NUMBER) {
        this.CARD_NUMBER = CARD_NUMBER;
    }

    public Long getSTART_TIME() {
        return START_TIME;
    }

    public void setSTART_TIME(Long START_TIME) {
        this.START_TIME = START_TIME;
    }

    public Long getEND_TIME() {
        return END_TIME;
    }

    public void setEND_TIME(Long END_TIME) {
        this.END_TIME = END_TIME;
    }

    public String getKEY_TIME() { return getSTART_TIME() + "_" + getEND_TIME();}
}
