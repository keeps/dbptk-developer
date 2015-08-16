package dk.magenta.siarddk;

public class test {

  public static void main(String[] args) {
    SIARDDKsql99ToXsdType converter = new SIARDDKsql99ToXsdType();
    System.out.println(converter.convert("NUMERIC"));
  }

}
