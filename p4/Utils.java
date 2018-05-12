import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Utils {
  public static ProjectLib PL;

  /**
   * Serialize the object into byte array
   * @param o
   * @return
   */
  public static byte[] serialize(Object o) {
    byte[] bytes = null;
    try {
      ByteArrayOutputStream bs = new ByteArrayOutputStream();
      ObjectOutputStream os = new ObjectOutputStream(bs);
      os.writeObject(o);
      bytes = bs.toByteArray();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return bytes;
  }

  /**
   * Deserialize the byte array into object
   * @param bytes
   * @return
   */
  public static Object deSerialize(byte[] bytes) {
    Object o = null;
    ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
    try {
      ObjectInputStream si = new ObjectInputStream(bi);
      o = si.readObject();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return o;
  }
  
  /**
   * Flush byte array into the disk
   * @param filename
   * @param bytes
   */
  public static void write_object(String filename, Object o) {
    try {
      FileOutputStream fos = new FileOutputStream(filename);
      ObjectOutputStream oos = new ObjectOutputStream(fos);
      oos.writeObject(o);
      oos.close();
      PL.fsync();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Read byte array from file
   * @param filename
   * @param bytes
   */  
  public static Object read_object(String filename) {
    try {
      FileInputStream fis = new FileInputStream(filename);
      ObjectInputStream ois = new ObjectInputStream(fis);
      return ois.readObject();
    } catch (IOException e) {
      // do nothing
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    
    return null;
  }
  
  /**
   * Read file to byte array
   * @param filename
   * @return
   */
  public static byte[] read_image(String filename){
    try {
      Path path = Paths.get(filename);
      return Files.readAllBytes(path);
    } catch (IOException e) {
      System.out.println("No tmp img" + filename);
    }
    return null;
  }
  
  /**
   * Flush the binary image into the disk
   * @param filename
   * @param img
   */
  public static void write_Image(String filename, byte[] img) {
    try {
      FileOutputStream fos = new FileOutputStream(filename);
      fos.write(img, 0, img.length);
      fos.flush();
      fos.close();
      PL.fsync();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
