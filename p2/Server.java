import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

public class Server extends UnicastRemoteObject implements ServerI{
  private final int EXIT_SUCCESS = 0;
  
  protected Server() throws RemoteException {
    super();
  }

  static{
    timestamps = new HashMap<String, Integer>();
  }
  
  private static String fileroot = "serverFiles";
  private static int port = 3000;
  private static HashMap<String, Integer> timestamps;
  
  @Override
  public synchronized String fileMeta(String path) throws RemoteException {
    System.err.println("[SERVER] fileMeta called (path: "+ path+" ) :TS" + System.currentTimeMillis());
    long ts = timestamp(path);
    long fl = fileLength(path);
    System.err.println("reply: " + ts + " " + fl);
    return ts + " " + fl;
  }
  
  /**
   * Get the time stamp of a certain file
   * @param path
   * @return
   */
  public synchronized long timestamp(String path){
//    System.err.println("[SERVER] timestamp called (path: "+ path+" )");
    String localpath = fileroot + path;
    if(!timestamps.containsKey(path)) timestamps.put(path, 1);
    return timestamps.get(path);
  }

  /**
   * Get the file length. Return -1 if not existed
   * @param
   * @return
   */
  public synchronized long fileLength(String path){
//    System.err.println("[SERVER] fileLength called (path: "+ path+" )");
    path = fileroot + path;
    File file = new File(path);
    if(!file.exists()) return -2; // File not found
    return file.length();
  }
  
  /**
   * Read original file from server 
   * @param path
   * @param off
   * @param len
   * @return new byte[0]   if we reach the end of file
   *         null          if we have an error reading files
   *         new byte[len] if we read len bytes of the file
   */
  public synchronized byte[] readFile(String path, int len, int skip){
    System.err.println("[Server] readFile called :TS" + System.currentTimeMillis());
    System.err.println("[Server] path: " + path);
    System.err.println("[Server] len:  " + len);
    System.err.println("[Server] skip: " + skip);
    path = fileroot + path;
    System.err.println("[Server] Modified path: " + path);
    try {
      File file = new File(path);
      System.err.println("[Server] File Length: " + file.length());
      
      RandomAccessFile raf = new RandomAccessFile(file, "r");
      byte[] B = new byte[len];
      raf.skipBytes(skip);
      int readLen = raf.read(B, 0, len);
      
      if(readLen == -1) readLen = 0;
      
      byte[] readB = new byte[readLen];
      
      for (int i = 0; i < readB.length; i++) {
        readB[i] = B[i];
      }
      
      raf.close();
      return readB;

    } catch (FileNotFoundException e) {
      System.err.println("[SERVER] file not found");
      return null;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
  
  /**
   * Write the bytes to the original server
   * @param b
   * @param off
   * @param len
   * @return
   */
  public synchronized boolean writeFile(String path, byte b[], 
                                        int len, int skip){
    System.err.println("[Server] writeFile called :TS" + System.currentTimeMillis());
    System.err.println("[Server] path: " + path);
    System.err.println("[Server] len:  " + len);
    System.err.println("[Server] skip: " + skip);
    String localpath = fileroot + path;
    System.err.println("[Server] Modified path: " + path);
    try {
      File file = new File(localpath);
      RandomAccessFile raf = new RandomAccessFile(file, "rw");
      raf.skipBytes(skip);
      raf.write(b, 0, len);
      raf.close();
      
      if(!timestamps.containsKey(path)) timestamps.put(path, 0);
      timestamps.put(path, timestamps.get(path)+1);
      
      return true;
      
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return false;
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }
  
  /**
   * Remove the file on the server
   * @param path
   */
  public synchronized int removeFile(String path){
    System.err.println("[Server] removeFile called :TS" + System.currentTimeMillis());
    System.err.println("[Server] path: " + path);
    String local_path = fileroot + path;
    File file = new File(local_path);
    if(!file.exists()) return -2; // File not found
    
    file.delete();
    
    if(!timestamps.containsKey(path)) timestamps.put(path, 1);
    
    timestamps.put(path, timestamps.get(path) + 100);
    
    System.err.println("[Server] TS: " + timestamps.get(path));
    
    return EXIT_SUCCESS;
  }
  
  public static void main(String[] args) throws RemoteException, 
                                                MalformedURLException {
    
    if(args.length != 2) return;
    
    port = Integer.parseInt(args[0]);
    fileroot = args[1] ;
    
    // make server directory
    File dir = new File(fileroot);
    dir.mkdir();
    
    fileroot = fileroot + "/";
    
    System.err.println("Server fileroot: " + fileroot);
    System.err.println("Server port:     " + port);
    
    LocateRegistry.createRegistry(port);
    
    Server server = new Server();
    
    String url = "//127.0.0.1:" + port + "/original";
    Naming.rebind(url, server);
  }

}

