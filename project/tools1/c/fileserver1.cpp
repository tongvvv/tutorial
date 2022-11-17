/*
 * 程序名：fileserver.cpp，文件传输的服务端
*/
#include "_public.h"

CLogFile logfile;      // 服务程序的运行日志。
CTcpServer TcpServer;  // 创建服务端对象。

struct st_arg
{
  int clienttype;          //服务方式 1-文件上传， 2-文件下载
  char ip[31];             //服务端ip
  int port;                //服务端端口
  int ptype;               //文件上传成功后文件的处理方式，1-删除文件，2-移动到备份目录
  char clientpath[301];    //本地文件存放的根目录
  char clientpathbak[301]; //本地文件上传成功后，本地文件的备份目录，当ptype==2时有效
  bool andchild;           //是否上传clientpath目录下各级子目录的文件
  char matchname[301];     //待上传文件名的匹配规则，如"*.txt,*.xml"
  char srvpath[301];       //服务端文件存放的根目录
  int timeval;             //扫描本地目录文件的时间间隔，单位：秒
  int timeout;             //进程心跳的超时时间
  char pname[51];          //进程名，建议用"tcpputfiles_后缀"的方式
}starg;

void FathEXIT(int sig);  // 父进程退出函数
void ChldEXIT(int sig);  // 子进程退出函数。

//把xml解析到参数starg结构体中
bool _xmltoarg(char* strxmlbuffer);

// 子进程与客户端进行通讯，处理业务。
char strrecvbuffer[1024],strsendbuffer[1024];

// 登录业务处理函数。
bool ClientLogin();
 
//上传文件的主函数
void RecvFilesMain();

int main(int argc,char *argv[])
{
  if (argc!=3)
  {
    printf("Using:./fileserver port logfile \nExample:./fileserver 5005 /tmp/fileserver.log \n\n"); return -1;
  }

  // 关闭全部的信号和输入输出。
  // 设置信号,在shell状态下可用 "kill + 进程号" 正常终止些进程
  // 但请不要用 "kill -9 +进程号" 强行终止
  CloseIOAndSignal(); signal(SIGINT,FathEXIT); signal(SIGTERM,FathEXIT);

  if (logfile.Open(argv[2],"a+")==false) { printf("logfile.Open(%s) failed.\n",argv[2]); return -1; }

  // 服务端初始化。
  if (TcpServer.InitServer(atoi(argv[1]))==false)
  {
    logfile.Write("TcpServer.InitServer(%s) failed.\n",argv[1]); return -1;
  }

  while (true)
  {
    // 等待客户端的连接请求。
    if (TcpServer.Accept()==false)
    {
      logfile.Write("TcpServer.Accept() failed.\n"); FathEXIT(-1);
    }

    logfile.Write("客户端（%s）已连接。\n",TcpServer.GetIP());

    /*
    if (fork()>0) { TcpServer.CloseClient(); continue; }  // 父进程继续回到Accept()。
   
    // 子进程重新设置退出信号。
    signal(SIGINT,ChldEXIT); signal(SIGTERM,ChldEXIT);

    TcpServer.CloseListen();
    */
   
    //子进程与客户端通信
    
    //处理登录客户端的登录报文
    if(ClientLogin() == false) ChldEXIT(-1);    
    //如果clienttype==1,调用上传文件主函数
    if(starg.clienttype==1) { RecvFilesMain();}
    //如果clienttype==2,调用下载文件主函数

    ChldEXIT(0);
  }
}

// 父进程退出函数。
void FathEXIT(int sig)  
{
  // 以下代码是为了防止信号处理函数在执行的过程中被信号中断。
  signal(SIGINT,SIG_IGN); signal(SIGTERM,SIG_IGN);

  logfile.Write("父进程退出，sig=%d。\n",sig);

  TcpServer.CloseListen();    // 关闭监听的socket。

  kill(0,15);     // 通知全部的子进程退出。

  exit(0);
}

// 子进程退出函数。
void ChldEXIT(int sig)  
{
  // 以下代码是为了防止信号处理函数在执行的过程中被信号中断。
  signal(SIGINT,SIG_IGN); signal(SIGTERM,SIG_IGN);

  logfile.Write("子进程退出，sig=%d。\n",sig);

  TcpServer.CloseClient();    // 关闭客户端的socket。

  exit(0);
}

//登录
bool ClientLogin()
{
  memset(strrecvbuffer,0,sizeof(strrecvbuffer));
  memset(strsendbuffer,0,sizeof(strsendbuffer));

  if(TcpServer.Read(strrecvbuffer,20)==false)
  {
    logfile.Write("TcpServer.Read() failed.\n"); return false;
  }
  logfile.Write("strrecvbuffer=%s\n",strrecvbuffer);

  _xmltoarg(strrecvbuffer);
  
  if(starg.clienttype !=1 && starg.clienttype !=2)
  {
     strcpy(strsendbuffer,"failed");
  }
  else
  {
     strcpy(strsendbuffer,"ok");
  }
 
  if(TcpServer.Write(strsendbuffer)==false)
  {
    logfile.Write("TcpServer.Writr() failed.\n"); return false;
  }

  logfile.Write("%s login strsendbuffer=%s\n",TcpServer.GetIP(),strsendbuffer);

  return true;
}

//把xml解析到参数starg结构体中
bool _xmltoarg(char* strxmlbuffer)
{
  memset(&starg,0,sizeof(struct st_arg));

  GetXMLBuffer(strxmlbuffer,"clienttype",&starg.clienttype);
  GetXMLBuffer(strxmlbuffer,"ptype",&starg.ptype);
  GetXMLBuffer(strxmlbuffer,"clientpath",starg.clientpath);
  GetXMLBuffer(strxmlbuffer,"andchild",&starg.andchild);
  GetXMLBuffer(strxmlbuffer,"matchname",starg.matchname);
  GetXMLBuffer(strxmlbuffer,"srvpath",starg.srvpath);

  GetXMLBuffer(strxmlbuffer,"timeval",&starg.timeval);
  if(starg.timeval <1) { logfile.Write("timeval is too small"); return false;}
  if(starg.timeval > 30) { starg.timeval = 30;}

  GetXMLBuffer(strxmlbuffer,"timeout",&starg.timeout);
  if(starg.timeout==0) { logfile.Write("timeout is null"); return false;}
  if(starg.timeout < 50) { starg.timeout = 50 ; }

  GetXMLBuffer(strxmlbuffer,"pname",starg.pname);
  strcat(starg.pname,"_srv");  

  return true;
}

//上传文件的主函数
void RecvFilesMain()
{
  memset(strrecvbuffer,0,sizeof(strrecvbuffer));
  memset(strsendbuffer,0,sizeof(strsendbuffer));
 
  //客户端每timeval扫描一次文件，这里只要比timeval大几秒就行了
  if(TcpServer.Read(strrecvbuffer,starg.timeval+10) == false) 
  {
    logfile.Write("TcpServer.Read() failed.\n");  return;
  }
  logfile.Write("strrecvbuffer=%s.\n",strrecvbuffer); 
  
  //处理心跳报文
  if( strcmp(strrecvbuffer,"<activetest>ok</activetest>") == 0)
  {
    strcpy(strsendbuffer,"ok");
    logfile.Write("strsendbuffer=%s.\n",strsendbuffer); 
    if(TcpServer.Write(strsendbuffer)==false)
    {
      logfile.Write("TcpServer.Write() failed.\n"); return;
    }
  }
  
  //处理上传文件的请求报文
}









