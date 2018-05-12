import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * @author Tengjiao
 *
 */
public class Cache {
  private int TOTAL_SIZE = 0;
  public int remained_size = 0;
  public int used_size = 0;
  public int being_used_size = 0;
  public int evict_available_size = 0;
  
  public HashMap<String, Integer> file_len = new HashMap<String, Integer>();
  public HashMap<String, Integer> file_used = new HashMap<String, Integer>();
  private ArrayList<String> files = new ArrayList<String>();
  public HashSet<String> rmfiles = new HashSet<String>();
  
  public Cache() {
    super();
  }

  public Cache(int TOTAL_SIZE) {
    super();
    this.TOTAL_SIZE = TOTAL_SIZE;
    this.remained_size = TOTAL_SIZE;
  }
  
  /**
   * Add a new cache block for the file named path, evict victims if necessary.
   * Caller should guarantee that there is enough room to insert this cache block 
   * @param path
   * @param new_len
   */
  public synchronized void add(String path, int new_len){
    
    if(files.contains(path)){ // UPDATE existed cache
      Integer old_len = file_len.get(path);
      Integer old_used = file_used.get(path);
      Integer new_used = old_used + 1;
      updateLen(path, old_len, new_len, old_used, new_used);
      update(path);
      
      if(remained_size < 0) remove(Math.abs(remained_size));
    }
    
    if(!files.contains(path)){ // INSERT new cache
      Integer old_len = 0;
      Integer old_used = 0;
      Integer new_used = 1;
//      file_used.put(path, true);
      file_len.put(path, new_len);
      updateLen(path, old_len, new_len, old_used, new_used);
      
      files.add(path);
      update(path);
      
      if(remained_size < 0) remove(Math.abs(remained_size));
      
    }

//    System.err.println("[CACHE] cache added ");
//    System.err.println("[CACHE] file_len " + file_len);
//    System.err.println("[CACHE] file_used " + file_used);
//    System.err.println("[CACHE] remained_size : " + remained_size);
//    System.err.println("[CACHE] used_size : " + used_size);
//    System.err.println("[CACHE] being_used_size : " + being_used_size);
//    System.err.println("[CACHE] evict_available_size : " + evict_available_size);
  }
  
  /**
   * Remove unused cache so that remained_size >= size_needed.
   * Remove in LRU fashion: latest piece is put at last, so we remove from beginning
   * @param size_needed
   */
  private synchronized void remove(int size_needed){
    HashSet<String> remove_set = new HashSet<String>();
    
    for (String file : files) {
      if(file_used.get(file) > 0) continue;
      
      remove_set.add(file);
      size_needed -= file_len.get(file);
      
      if(size_needed <= 0) break;
    }
    
    for (String rmfile: remove_set) remove(rmfile);
  }
  
  /**
   * Remove the cache file named path
   * @param path
   */
  public synchronized void remove(String path){
    if(!files.contains(path)) return;
    
    int old_used = file_used.get(path);
    int new_used = 0;
    int old_len = file_len.get(path);
    int new_len = 0;
    updateLen(path, old_len, new_len, old_used, new_used);
    
    files.remove(path);
    file_len.remove(path);
    file_used.remove(path);
    rmfiles.add(path);
    file_used.put(path, 0);
    
//    System.err.println("[CACHE] remove ");
//    System.err.println("[CACHE] old_len " + old_len);
//    System.err.println("[CACHE] new_len " + new_len);
//    System.err.println("[CACHE] file_len " + file_len);
//    System.err.println("[CACHE] file_used " + file_used);
//    System.err.println("[CACHE] remained_size : " + remained_size);
//    System.err.println("[CACHE] used_size : " + used_size);
//    System.err.println("[CACHE] being_used_size : " + being_used_size);
//    System.err.println("[CACHE] evict_available_size : " + evict_available_size);
  }
  
  /**
   * Update the cache block in LRU fashion, bring the hit piece to the back
   * @param path
   */
  public synchronized void update(String path){
    if( !file_used.containsKey(path) ) file_used.put(path, 1);
    else file_used.put(path, file_used.get(path) + 1);
    
    files.remove(path);
    files.add(path);
//    System.err.println("[CACHE] cache update ");
//    System.err.println("[CACHE] file_len " + file_len);
//    System.err.println("[CACHE] file_used " + file_used);
//    System.err.println("[CACHE] remained_size : " + remained_size);
//    System.err.println("[CACHE] used_size : " + used_size);
//    System.err.println("[CACHE] being_used_size : " + being_used_size);
//    System.err.println("[CACHE] evict_available_size : " + evict_available_size);
  }

  /**
   * Update the created file length from 0 to real length
   * @param path
   * @param new_len
   */
  public synchronized void updateCreateLen(String path, int new_len){
//    System.out.println("updateCreateLen, path:" + path);
    // If the length is not zero, this is not a created file
    // We do not have to care about this update
    if(!file_len.containsKey(path)){
      System.err.println("No such file cannot update, path" + path);
      return;
    } 
    Integer old_len = file_len.get(path);
    Integer old_used = file_used.get(path);
    Integer new_used = file_used.get(path);
    updateLen(path, old_len, new_len, old_used, new_used);
    
//    System.err.println("[CACHE] updateCreateLen ");
//    System.err.println("[CACHE] old_len " + old_len);
//    System.err.println("[CACHE] new_len " + new_len);
//    System.err.println("[CACHE] file_len " + file_len);
//    System.err.println("[CACHE] file_used " + file_used);
//    System.err.println("[CACHE] remained_size : " + remained_size);
//    System.err.println("[CACHE] used_size : " + used_size);
//    System.err.println("[CACHE] being_used_size : " + being_used_size);
//    System.err.println("[CACHE] evict_available_size : " + evict_available_size);
    
    file_len.put(path, new_len);
    if(remained_size < 0){
      // still buggy
//      System.err.println("[CACHE] update create evict ");      
      remove(Math.abs(remained_size));
    }
  }
  
  /**
   * Set a cache block as unused
   * @param path
   */
  public synchronized void setUnused(String path){
    if(!files.contains(path)) {
      System.err.println("[CACHE] set removed file as unused");
      return;
    }
  
    Integer old_used = file_used.get(path);
    Integer new_used = old_used - 1;
    Integer old_len = file_len.get(path);
    Integer new_len = file_len.get(path);
    updateLen(path, old_len, new_len, old_used, new_used);
    
    file_used.put(path, file_used.get(path) - 1);
    if(file_used.get(path) < 0)
      file_used.put(path, 0);
    
    files.remove(path);
    files.add(path);
    
//    System.err.println("[CACHE] cache unused ");
//    System.err.println("[CACHE] file_len " + file_len);
//    System.err.println("[CACHE] file_used " + file_used);
//    System.err.println("[CACHE] remained_size : " + remained_size);
//    System.err.println("[CACHE] used_size : " + used_size);
//    System.err.println("[CACHE] being_used_size : " + being_used_size);
//    System.err.println("[CACHE] evict_available_size : " + evict_available_size);
  }

  public synchronized boolean isUsed(String path){
    if(!file_used.containsKey(path)) return false;
    else return file_used.get(path) > 0;
  }
  
  public synchronized HashSet<String> getRmfiles() {
    return this.rmfiles;
  }
  
  /**
   * Return the possible caching size: remained used ones + used evictable ones
   * @return
   */
  public synchronized int availableSize(){
    return remained_size + evict_available_size;
  }

  /**
   * Update the cache parameters according to the new information
   * @param path
   * @param old_len
   * @param new_len
   * @param old_used
   * @param new_used
   */
  private synchronized void updateLen(String path, int old_len, int new_len, int old_used, int new_used){
    used_size = used_size - old_len + new_len;
    remained_size = TOTAL_SIZE - used_size;
    
    if(old_used <= 0 && new_used > 0){ // previous not used, currently used
      being_used_size += new_len;
      evict_available_size = used_size - being_used_size;
    } 
    else if(old_used > 0 && new_used > 0){ // previous used, currently used
      being_used_size = being_used_size - old_len + new_len;
      evict_available_size = used_size - being_used_size;
    }
    else if(old_used > 0 && new_used <= 0){ // previous used, currently not used
      being_used_size = being_used_size - old_len;
      evict_available_size = used_size - being_used_size;
    }
    else if(old_used <= 0 && new_used <= 0){ // previous not used, currently not used
      being_used_size = being_used_size;
      evict_available_size = used_size - being_used_size;
    }

//    System.err.println("[CACHE] cache update length ");
//    System.err.println("[CACHE] file_len " + file_len);
//    System.err.println("[CACHE] file_used " + file_used);
//    System.err.println("[CACHE] old_len : " + old_len);
//    System.err.println("[CACHE] new_len : " + new_len);
//    System.err.println("[CACHE] old_used : " + old_used);
//    System.err.println("[CACHE] new_used : " + new_used);
//    System.err.println("[CACHE] remained_size : " + remained_size);
//    System.err.println("[CACHE] used_size : " + used_size);
//    System.err.println("[CACHE] being_used_size : " + being_used_size);
//    System.err.println("[CACHE] evict_available_size : " + evict_available_size);
  }
  
  // Naiive test
  public static void main(String[] args) {
    test1();
  }

  private static void test1() {
    Cache cache = new Cache(10);
    cache.add("a", 4);
    cache.add("a", 5);
    cache.add("b", 3);
    cache.setUnused("a");
    cache.setUnused("a");
    cache.setUnused("b");
    cache.add("d", 3);
    System.out.println(cache.getRmfiles());

  }
}

