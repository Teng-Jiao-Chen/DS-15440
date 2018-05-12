import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ServerI extends Remote {
  enum Role implements Serializable{
    MASTER, // one of front members, responsible for global data
    FRONT, MIDDLE, CACHE, 
    DEAD // the machine with this id is dead
  }
  
  class Job implements Serializable{
    private static final long serialVersionUID = 1L;
    public Role role;
    public int VMid;
  }
  
  public Job assign(int VmID) throws RemoteException;
  public void addRequest(Cloud.FrontEndOps.Request r) throws RemoteException;
  public Cloud.FrontEndOps.Request getRequest() throws RemoteException;
  public void removeVM(int VmID) throws RemoteException;
}

