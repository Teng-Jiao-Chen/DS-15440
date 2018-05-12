import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ServerI extends Remote {
  public String fileMeta(String path) throws RemoteException;
  public long timestamp(String path) throws RemoteException;
  public long fileLength(String path) throws RemoteException;
  public byte[] readFile(String path, int len, int skip) throws RemoteException;
  public boolean writeFile(String path, byte b[], int len, int skip) throws RemoteException;
  public int removeFile(String path) throws RemoteException;
}
