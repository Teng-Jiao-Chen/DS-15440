import java.io.*;
import java.nio.file.Files;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.*;

//Errors.EBADF     code: Bad file descriptor
//Errors.EBUSY     code: Resource busy
//Errors.EEXIST    code: File exists
//Errors.EINVAL    code: Invalid parameter
//Errors.EISDIR    code: Is a directory
//Errors.EMFILE    code: Too many open files
//Errors.ENOENT    code: Not found
//Errors.ENOMEM    code: Out of memory
//Errors.ENOSYS    code: Function not implemented
//Errors.ENOTDIR   code: Not a directory
//Errors.ENOTEMPTY code: Directory not empty
//Errors.EPERM     code: Not permitted

class Proxy {
  
  private static String serverIP = "127.0.0.1";
  private static int port = 3000;
  private static String cacheRoot = "tmp/cache";
  private static int cacheSize = 20000000;
  private static Cache cache = null;
  private static ServerI server = null;
  public static final int MAX_OPEN = 1000;
  public static final int EXIT_SUCCESS = 0;
  public static final int FD_OFFSET = 3000; // distinguish local and cloud files
  public static int fd_counter = 0;
  public static HashMap<Integer, File> fd_map = new HashMap<Integer, File>();
  public static HashMap<Integer, RandomAccessFile> fdrw_map = new HashMap<Integer, RandomAccessFile>(); 
  public static HashMap<Integer, File> fd_pmap = new HashMap<Integer, File>();
  public static HashMap<Integer, RandomAccessFile> fdrw_pmap = new HashMap<Integer, RandomAccessFile>(); 
  public static HashMap<String, Long> timestamps = new HashMap<String, Long>();
  
  private static class FileHandler implements FileHandling {
    
    /**
     * Load the fresh copy from server into the proxy cache
     * @param path PATH WITHOUT CACHEROOT
     * @return
     */
    public synchronized int load_cache(String path, int fileLength, boolean dirty){
      System.err.println("[Proxy] load_cache called  :TS" + System.currentTimeMillis());
      System.err.println("[Proxy] path: " + path);
      System.err.println("[Proxy] cacheRoot: " + cacheRoot);

      if(fileLength <= 0) return Errors.ENOENT;
      
      // mkdir if parent not yet exist
      File _file = new File(cacheRoot, path);
      if(!_file.getParentFile().exists())
        new File(_file.getParent()).mkdirs();
      
      String localpath = cacheRoot + path;
      if(dirty) localpath = localpath + "__dirty";
      
      try{
        int skip = 0;
        int len = 1000000;
        byte[] b;
        while( (b = server.readFile(path, len, skip)) != null){
          File file = new File(localpath);
          RandomAccessFile raf = new RandomAccessFile(file, "rw");
          raf.skipBytes(skip);
          raf.write(b, 0, b.length);
          raf.close();
          
          skip += b.length;
          if(b.length == 0) break;
          if(skip == fileLength) break;
          
        }
      } catch (RemoteException e) {
        e.printStackTrace();
      } catch (FileNotFoundException e) {
        e.printStackTrace();
        return Errors.ENOENT;
      } catch (IOException e) {
        e.printStackTrace();
      }
      
      return EXIT_SUCCESS;
    }
    
    /**
     * Push cache from the local dirty proxy cache to the server
     * This cache is pushed from the private view
     * @param path!! THE RAW PATH WITHOUT cacheRoot
     * @return
     */
    public synchronized int push_cache(int fd, String path){
      System.err.println("[Proxy] push_cache called :TS" + System.currentTimeMillis());
      System.err.println("[Proxy] path: " + path);
      String pathLocal = cacheRoot + path + "_by_" + fd;

      System.err.println("[Proxy] pathLocal: " + pathLocal);
      try{
        File file = new File(pathLocal);
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        
        int skip = 0;
        int len = 1000000;
        int readLen = 0;
        byte[] b = new byte[len];
        
        while( (readLen = raf.read(b, 0, len)) != -1){
//          Long localtime = timestamps.get(path);
//          System.err.println("[Proxy] localtime:" + (localtime + 1));
//          timestamps.put(path, (timestamps.get(path)) + 1);
          
          server.writeFile(path, b, readLen, skip);
          skip += readLen;
        }
        
        raf.close();
        
        file.delete();
        
      } catch (RemoteException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
      
      return EXIT_SUCCESS;
    }
    
    /**
     * Remove the remote copy of the server file
     * @param path
     * @return
     */
    public synchronized int push_remove(String path){
      try {
        return server.removeFile(path);
      } catch (RemoteException e) {
        e.printStackTrace();
      }
      
      return EXIT_SUCCESS;
    }
    
    /**
     * Remove the local copy of the cache file
     * @param path
     * @return
     */
    public synchronized int remove_cache(String path){
      System.err.println("[proxy] remove_cache " + path  +" :TS" + System.currentTimeMillis());
      
//      System.err.println("cache.file_len " + cache.file_len);
//      System.err.println("cache.file_used " + cache.file_used);      
      
      String local_path = cacheRoot + path;
      File file = new File(local_path);
      
      if(!file.exists()) return EXIT_SUCCESS;
      if(cache.isUsed(path)) return EXIT_SUCCESS;
      if(file.isDirectory()) return Errors.EISDIR;
      
      cache.remove(path);
      file.delete();
      timestamps.remove(path);
      
      return EXIT_SUCCESS;
    }
    
    public synchronized int open(String path, OpenOption o) {
      synchronized (FileHandler.class) {
        System.err.println("[proxy] open path: " + path + "OpenOption: " + o + ":TS" + System.currentTimeMillis());
        
        // Check if there is ".." tricks to unpermitted folders
        try {
          String canonicalPath = new File(cacheRoot + path).getCanonicalPath();
          if(!canonicalPath.contains(cacheRoot)) return Errors.EPERM;
        } catch (IOException e1) {
          e1.printStackTrace();
        }
        
        // If we open too many files, Error Max Open Error
        if(fd_map.keySet().size() > MAX_OPEN) return Errors.EMFILE;
        
        // fileLength will return -2, if the file does not exist
        // This information is used for later open option
        int fileLength = -1;
        long timestamp = -1;
        
        try {
          String ts_fl = server.fileMeta(path);
          String[] paras = ts_fl.split(" ");
          timestamp = Integer.parseInt(paras[0]);
          fileLength = Integer.parseInt(paras[1]);
          
  //        timestamp = server.timestamp(path);
          // PROBLEMATIC
  //        if(fileLength < 0) return fileLength; // if negative, return error code
        } catch (RemoteException e) {
          e.printStackTrace();
        }
  
        if(timestamps.containsKey(path)){
          System.err.println("local  TS: " + timestamps.get(path) + " global TS: " + timestamp);
        }
        
        // Clear out possible available size
        cache.add("dummy", 0);
        clearRmset();
        // If cache is not enough for holding files, Error Resource Busy
  //      if(cache.availableSize() < fileLength) {
  //        System.err.println("PROXY availableSize: " + cache.availableSize());
  //        System.err.println("PROXY fileLength: " + fileLength);
  //        return Errors.EBUSY;
  //      }
        
        boolean dirtyRead = false;
        
        // load into cache if non-exist or stale
        if(!timestamps.containsKey(path) || timestamps.get(path) < timestamp){
          System.err.println("Loading cache from server");
          
  //        try {
  //          fileLength = (int)server.fileLength(path);
  //        } catch (RemoteException e) {
  //          e.printStackTrace();
  //        }
          
  //        if(timestamps.containsKey(path))
  //          System.err.println("[Proxy] Cache miss: local time " + timestamps.get(path));
  
          // If Open Option is CREATE_NEW, no need to load cache from server
          if(!((o == OpenOption.CREATE_NEW) 
                || (o == OpenOption.READ && cache.isUsed(path)))){
            load_cache(path, fileLength, false);
            timestamps.put(path, timestamp);
          }
          
          if(o == OpenOption.READ && cache.isUsed(path)&&timestamps.containsKey(path)){
            load_cache(path, fileLength, true);
            dirtyRead = true;
          }
          
          if(!timestamps.containsKey(path)) timestamps.put(path, (long) 1);
          
          if(fileLength <= 0){ // If server does not have this file
            cache.add(path, 0); // add a zero length cache block
          } else{
            cache.add(path, fileLength);
            clearRmset();
          }
          
  //        System.err.println("[Proxy] Cache miss: server time " + timestamp);
          
        }
        else if(timestamps.get(path) >= timestamp){ // if local copy is fresh
          fileLength = (int)new File(cacheRoot,path).length();
          cache.add(path, fileLength);
        }
        
        String local_path = cacheRoot + path; // localized the path
        
        if(dirtyRead) local_path = local_path + "__dirty";
        
        File file = new File(local_path);
  
        if(file.isDirectory()){
          fd_counter++;
          fd_map.put(fd_counter, file);
          return fd_counter + FD_OFFSET;
        }
        
        fd_counter++;
        String p_path = cacheRoot + path + "_by_" + fd_counter;
        File p_file = new File(p_path);
        
        switch (o) {
          case CREATE:
            
            // If server does not have the file, mark the file as modified
            if(fileLength < 0 && timestamps.get(path) > 0)
              timestamps.put(path, -timestamps.get(path));// NEGATIVE TIMESTAMP MEANS DIRTY
            
            try {
              fd_map.put(fd_counter, file);
              fd_pmap.put(fd_counter, p_file);
              
              if(file.exists()){
                Files.copy(file.toPath(), p_file.toPath());
                cache.add(path +"_by_"+fd_counter, (int)p_file.length());
                clearRmset();
                cache.setUnused(path);
              }else{
                cache.add(path +"_by_"+fd_counter, 0);
                cache.setUnused(path);
              }
              
              fdrw_pmap.put(fd_counter, new RandomAccessFile(p_file, "rw"));
            } catch (IOException e) {
              e.printStackTrace();
              return Errors.ENOENT;
            }
            
            break;
          case CREATE_NEW:
            // If server has the file, EXIST Error 
            if(file.exists() || fileLength >= 0) return Errors.EEXIST; 
            
            if(timestamps.get(path) > 0)
              timestamps.put(path, -timestamps.get(path));// NEGATIVE TIMESTAMP MEANS DIRTY
            
            try {
              fd_map.put(fd_counter, file);
              fd_pmap.put(fd_counter, p_file);
              fdrw_pmap.put(fd_counter, new RandomAccessFile(p_file, "rw"));
              cache.add(path +"_by_"+fd_counter, 0);
              cache.setUnused(path);
            } catch (FileNotFoundException e) {
              e.printStackTrace();
              return Errors.ENOENT;
            }
            
            break;
          case READ:
            // If file does not exist, Error NOT FOUND
            System.err.println("Read File Length: " + fileLength);
            if(!file.exists() || fileLength < 0) return Errors.ENOENT;
            if(!file.canRead()) return Errors.EPERM;
  
            try {
              fd_map.put(fd_counter, file);
              fdrw_map.put(fd_counter, new RandomAccessFile(file, "r"));
            } catch (FileNotFoundException e) {
              e.printStackTrace();
              return Errors.ENOENT;
            }
            
            break;
          case WRITE:
            // If file does not exist, Error NOT FOUND
            if(!file.exists() || fileLength < 0) return Errors.ENOENT;
            if(!file.canWrite()) return Errors.EPERM;
            
            // modified indicator won't be set until actual write
            
            try {
              fd_map.put(fd_counter, file);
              fd_pmap.put(fd_counter, p_file);
              fdrw_pmap.put(fd_counter, new RandomAccessFile(p_file, "rw"));
              Files.copy(file.toPath(), p_file.toPath());
              cache.add(path +"_by_"+fd_counter, (int)p_file.length());
              clearRmset();
              cache.setUnused(path);
            } catch (IOException e) {
              e.printStackTrace();
              return Errors.ENOENT;
            }
            
            break;
        }
        
        return fd_counter + FD_OFFSET;
      }
    }

    public synchronized int close(int fd) {
      System.err.println("[proxy] close " + fd + " :TS" + System.currentTimeMillis());

//      System.err.println("cache.file_len " + cache.file_len);
//      System.err.println("cache.file_used " + cache.file_used);
//      System.err.println("cache.rmfiles " + cache.rmfiles);
      
      if(fd >= FD_OFFSET) fd -= FD_OFFSET;
      
      if(!fd_map.containsKey(fd)) return Errors.EBADF;
      
      String path = fd_map.get(fd).getPath();
      path = path.substring(cacheRoot.length(), path.length());

      // If we have a private view, then we must have a CREATE OR WRITE
      if(fd_pmap.containsKey(fd)){
        // If there is a public view, delete it
//        if(fd_map.get(fd).exists()) fd_map.get(fd).delete();
        
        // Rename the private view name to public one
//        fd_pmap.get(fd).renameTo(fd_map.get(fd));
        
        // NEGATIVE TIMESTAMP MEANS DIRTY, WE HAVE TO WRITE IT BACK
        if(timestamps.get(path) < 0){
          System.err.println("Writing dirty content back to server...");
          timestamps.put(path, -timestamps.get(path)); // UNSET DIRTY BIT
          push_cache(fd, path);
          cache.remove(path + "_by_" + fd);
          cache.updateCreateLen(path, (int)fd_map.get(fd).length());
//          cache.setUnused(path + "_by_" + fd);
          clearRmset();
        }
      }else{
        cache.setUnused(path);
      }
      
      File file = fd_map.get(fd);
      fd_map.remove(fd);
      if(fd_pmap.containsKey(fd)) fd_map.remove(fd);
      
      try {

        if(fdrw_map.containsKey(fd)){
          fdrw_map.get(fd).close();
          fdrw_map.remove(fd);
        }

        if(fdrw_pmap.containsKey(fd)){
          fdrw_pmap.get(fd).close();
          fdrw_pmap.remove(fd);
        }
        
      } catch (IOException e) {
        e.printStackTrace();
        return Errors.ENOENT;
      }
      
      if(path.contains("__dirty")) file.delete();
      
      return EXIT_SUCCESS;
    }

    public synchronized long write(int fd, byte[] buf) {
      System.err.println("[proxy] write, fd:" +fd+", len: " +buf.length +" :TS" + System.currentTimeMillis());
      
      if(fd >= FD_OFFSET) fd -= FD_OFFSET;
      if(!fd_map.containsKey(fd)) return Errors.EBADF;
      
      File file = fd_pmap.get(fd);
      if(file.isDirectory()) return Errors.EISDIR;
      
      try {
        
        RandomAccessFile rFile = fdrw_pmap.get(fd);
        rFile.write(buf);
        
        String path = fd_map.get(fd).getPath();
        path = path.substring(cacheRoot.length(), path.length());
        
//        System.err.println("[Proxy] write path: " + path+"_by_"+fd);
//        System.err.println("[Proxy] write leng: " + (int)file.length());
//        System.err.println("cache.file_len " + cache.file_len);
//        System.err.println("cache.file_used " + cache.file_used);
        
        // NEGATIVE TIMESTAMP MEANS DIRTY
        if(timestamps.get(path) > 0)
          timestamps.put(path, -timestamps.get(path));

        cache.updateCreateLen(path+"_by_"+fd, (int)file.length());
        clearRmset();
      } catch (IOException e1) {
        e1.printStackTrace();
        return Errors.EBADF;
      } 
      
      return buf.length;
    }

    public synchronized long read(int fd, byte[] buf) {
      System.err.println("[proxy] read, fd:" +fd+", len: " +buf.length + " :TS" + System.currentTimeMillis());
      
      if(fd >= FD_OFFSET) fd -= FD_OFFSET;
      if(!fd_map.containsKey(fd)) return Errors.EBADF;

      File file = fd_map.get(fd);
      if(file.isDirectory()) return Errors.EISDIR;
      
      int length = 0;
      
      try {
        
        RandomAccessFile rFile = fdrw_map.get(fd);
        length = rFile.read(buf);
        
      } catch (IOException e1) {
        e1.printStackTrace();
        return Errors.EPERM;
      } 
      
      if(length == -1) return 0; // end of file
      else return length;
    }

    public synchronized long lseek(int fd, long pos, LseekOption o) {
      System.err.println("lseek  :TS" + System.currentTimeMillis());
      
      if(fd >= FD_OFFSET) fd -= FD_OFFSET;
      if(!fd_map.containsKey(fd)) return Errors.EBADF;
      
      File file = null;
      if(fd_pmap.containsKey(fd)) file = fd_pmap.get(fd);
      else file = fd_map.get(fd);
      
      if(file.isDirectory()) return Errors.EISDIR;
      
      RandomAccessFile rFile = null;
      if(fdrw_pmap.containsKey(fd)) rFile = fdrw_pmap.get(fd);
      else rFile = fdrw_map.get(fd);
      
      switch (o) {
        case FROM_START:
          try {
            rFile.seek(pos);
          } catch (IOException e) {
            e.printStackTrace();
            return Errors.EPERM;
          }
          break;
        case FROM_END:
          try {
            rFile.seek(rFile.length() - pos);
          } catch (IOException e) {
            e.printStackTrace();
            return Errors.EPERM;
          }
          
          break;
        case FROM_CURRENT:
          try {
            rFile.seek(rFile.getFilePointer() + pos);
          } catch (IOException e) {
            e.printStackTrace();
            return Errors.EPERM;
          }          
          break;
      }
      
      return EXIT_SUCCESS;
    }

    public synchronized int unlink(String path) {
      System.err.println("[proxy] unlink, path: "+path + "  :TS" + System.currentTimeMillis());
      
      String local_path = cacheRoot + path;
      File file = new File(local_path);
      
//      if(!file.exists()) return Errors.ENOENT;
      if(file.isDirectory()) return Errors.EISDIR;
      
      remove_cache(path);
      
      return push_remove(path);
    }

    public synchronized void clientdone() {
      System.err.println("client done");
      
      // One proxy might have many clients. 
      // You cannot close fd opened by others.
      
//      HashSet<Integer> fds = new HashSet<Integer>();
//      fds.addAll(fd_map.keySet());
//      
//      for(int fd : fds)
//        close(fd);
    }
    
    private synchronized void clearRmset() {
      HashSet<String> rmfiles = cache.getRmfiles();
      
      //remove the local copy of the evicted pieces
      for(String file : rmfiles) remove_cache(file);
      
      rmfiles.clear();
    }
  }

  private static class FileHandlingFactory implements FileHandlingMaking {
    
    public FileHandling newclient() {
      return new FileHandler();
    }
  }

  public static void main(String[] args) throws IOException, NotBoundException {
    if(args.length != 4) return;
    
    serverIP = args[0];
    port = Integer.parseInt(args[1]);
    cacheRoot = args[2] ;
    cacheSize = Integer.parseInt(args[3]);
    
    // make cache directory
    File dir = new File(cacheRoot);
    dir.mkdir();
    
    cacheRoot = cacheRoot + "/";
    
    String url = "//" + serverIP + ":" + port + "/original";
    server = (ServerI) Naming.lookup(url);
    cache = new Cache(cacheSize);
    
    FileHandlingFactory fhf = new FileHandlingFactory();
    RPCreceiver rpCreceiver = new RPCreceiver(fhf);

//    FileHandling nc = fhf.newclient();
//    
//    new Thread(new Runnable() {
//      
//      @Override
//      public void run() {
//        try {
//          Thread.sleep(3000);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//
//        int c1fd = nc.open("bar", FileHandling.OpenOption.CREATE);
//        nc.write(c1fd, "version1".getBytes());
//        byte[] buf1 = new byte[4096];
//        nc.read(c1fd, buf1);
//        
//        int c2fd = nc.open("bar", FileHandling.OpenOption.READ);
//        byte[] buf = new byte[4096];
//        nc.read(c2fd, buf);
//        System.out.println("c2 read : " + new String(buf));
//        nc.close(c2fd);
//        
//        int c3fd = nc.open("bar", FileHandling.OpenOption.CREATE);
//        nc.write(c3fd, "version2".getBytes());
//        nc.close(c3fd);
//        
//        int c4fd = nc.open("bar", FileHandling.OpenOption.READ);
//        byte[] buf2 = new byte[4096];
//        nc.read(c4fd, buf2);
//        System.out.println("c4 read : " + new String(buf2));
//        nc.close(c4fd);
//
//        nc.lseek(c1fd, 5, FileHandling.LseekOption.FROM_START);
//        nc.write(c1fd, "version1".getBytes());
//        nc.close(c1fd);
//        
//        int c5fd = nc.open("bar", FileHandling.OpenOption.READ);
//        byte[] buf3 = new byte[4096];
//        nc.read(c5fd, buf3);
//        System.out.println("c5 read : " + new String(buf3));
//        nc.close(c5fd);
//        
//      }
//    }).start();
    
    rpCreceiver.run();

  }
}



