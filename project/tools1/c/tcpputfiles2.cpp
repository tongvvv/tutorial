/*
 * 程序名：tcpputfiles.cpp，文件上传的客户端
*/
#include "_public.h"

bool ActiveTest();    // 心跳。
bool Login(const char* argv);    // 登录业务。
void EXIT(int sig); //信号2和15的处理函数
void _help();
bool _xmltoarg(char* strxmlbuffer); 
bool _tcpputfiles();  

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

CLogFile logfile;
CTcpClient TcpClient;
CPActive PActive;
char strrecvbuffer[1024]; //发送报文的buffer
char strsendbuffer[1024];//接收报文的buffer

int main(int argc,char *argv[])
{
  if (argc!=3)
  {
     _help();
     return -1;
  }

  /*CloseIOAndSignal();*/ signal(2,EXIT); signal(15,EXIT);

  if(logfile.Open(argv[1],"a+") == false)
  {
     printf("打开日志文件失败\n"); return -1;
  }

  if(_xmltoarg(argv[2]) == false) { logfile.Write("解析xml失败\n"); return -1; }

  //PActive.AddPInfo(starg.timeout,starg.pname);  //把进程的心跳信息写入共享内存

  // 向服务端发起连接请求。
  if (TcpClient.ConnectToServer(starg.ip,starg.port)==false)
  {
    logfile.Write("TcpClient.ConnectToServer(%s,%d) failed.\n",starg.ip,starg.port); EXIT(-1);
  }

  // 登录业务。
  if (Login(argv[2])==false) { logfile.Write("Login() failed.\n"); EXIT(-1); }

  while(true)
  {
    if(_tcpputfiles() == false) { logfile.Write("_tcpputfiles() == false\n"); EXIT(-1); }    

    sleep(starg.timeval);
    if(ActiveTest() == false) { logfile.Write("ActiveTest() == false\n"); break; }
  }


  EXIT(0);
}

void _help()
{
  putchar('\n');
  printf("Using:/project/tools1/bin/tcpputfiles logfilename xmlbuffer\n\n");
  printf("Example:/project/tools1/bin/procctl 20 /project/tools1/bin/tcpputfiles /tmp/tcpputfiles.log \"<ip>47.107.41.197</ip><port>5005</port><ptype>1</ptype><clientpath>/tmp/tcp/surfdata1</clientpath><clientpathbak>/tmp/tcp/surfdata1bak</clientpathbak><andchild>true</andchild><matchname>*.xml,*.csv</matchname><srvpath>/tmp/tcp/surfdata2</srvpath><timeval>10</timeval><timeout>50</timeout><pname>tcpputfiles_surfdata</pname>\" \n\n");

  printf("Example:/project/tools1/bin/procctl 20 /project/tools1/bin/tcpputfiles /tmp/tcpputfiles.log \"<ip>47.107.41.197</ip><port>5005</port><ptype>2</ptype><clientpath>/tmp/tcp/surfdata1</clientpath><clientpathbak>/tmp/tcp/surfdata1bak</clientpathbak><andchild>true</andchild><matchname>*.xml,*.csv</matchname><srvpath>/tmp/tcp/surfdata2</srvpath><timeval>10</timeval><timeout>50</timeout><pname>tcpputfiles_surfdata</pname>\" \n\n\n");

  printf("本程序是通用的功能模块,采用tcp协议把文件发送给服务端。\n");
  printf("logfilename 是本程序运行的日志文件。\n");
  printf("xmlbuffer为文件下载的参数，如下：\n");
  printf("<ip>47.107.41.197:21</ip> 服务端ip\n");
  printf("<port>5005</port> 服务端端口"); 
  printf("<ptype>1</ptype> 文件上传成功后文件的处理方式，1-删除文件，2-移动到备份目录\n");
  printf("<clientpath>/tmp/tcp/surfdata1</clientpath> 本地文件存放的根目录\n");
  printf("<clientpathbak>/tmp/tcp/surfdata1bak</clientpathbak> 本地文件上传成功后，本地文件的备份目录，当ptype==2时有效\n");
  printf("<andchild>true</andchild> 否上传clientpath目录下各级子目录的文件\n");
  printf("<matchname>*.xml,*.csv</matchname> 待上传文件的匹配规则。"\
           "不匹配的文件不会被下载，本字段尽可能设置精确，不建议用*匹配全部的文件\n");
  printf("<srvpath>/tmp/tcp/surfdata2</srvpath> 服务端文件存放的根目录\n");
  printf("<timeval>10</timeval> 扫描本地目录文件的时间间隔，单位：秒,取值在1-30之间\n");
  printf("<timeout>50</timeout> 进程心跳的超时时间，视文件大小和网络带宽而定，建议设置50以上。\n");
  printf("<pname>tcpputfiles_surfdata</pname> 进程名，建议用\"tcpputfiles_后缀\"的方式\n\n\n");
}

bool _xmltoarg(char* strxmlbuffer)
{
  memset(&starg,0,sizeof(struct st_arg)); 

  GetXMLBuffer(strxmlbuffer,"ip",starg.ip);
  if(strlen(starg.ip) == 0) { logfile.Write("ip is null \n"); return false;}

  GetXMLBuffer(strxmlbuffer,"port",&starg.port);
  if(starg.port == 0 ) { logfile.Write("port is null\n"); return false;}

  GetXMLBuffer(strxmlbuffer,"ptype",&starg.ptype);
  if(starg.ptype != 1 && starg.ptype != 2) { logfile.Write("ptypr not in (1,2)\n"); return false;}

  GetXMLBuffer(strxmlbuffer,"clientpath",starg.clientpath);
  if(strlen(starg.clientpath) == 0) { logfile.Write("clientpath is null \n"); return false;}
 
  GetXMLBuffer(strxmlbuffer,"clientpathbak",starg.clientpathbak);
  if(starg.ptype==2 && (strlen(starg.clientpathbak)==0) ) { logfile.Write("clientpathbak is null \n"); return false;}

  GetXMLBuffer(strxmlbuffer,"andchild",&starg.andchild);

  GetXMLBuffer(strxmlbuffer,"matchname",starg.matchname);
  if(strlen(starg.matchname) == 0) { logfile.Write("matchname is null \n"); return false;} 

  GetXMLBuffer(strxmlbuffer,"srvpath",starg.srvpath);
  if(strlen(starg.srvpath) == 0) { logfile.Write("srvpath is null \n"); return false;}  

  GetXMLBuffer(strxmlbuffer,"timeval",&starg.timeval);
  if(starg.timeval <1) { logfile.Write("timeval is too small"); return false;} 
  if(starg.timeval > 30) { starg.timeval = 30;}

  GetXMLBuffer(strxmlbuffer,"timeout",&starg.timeout);
  if(starg.timeout==0) { logfile.Write("timeout is null"); return false;}
  if(starg.timeout < 50) { starg.timeout = 50 ; }

  GetXMLBuffer(strxmlbuffer,"pname",starg.pname);
  if(strlen(starg.pname) == 0) { logfile.Write("pname is null \n"); return false;}  

  return true;
}
        
// 心跳。 
bool ActiveTest()    
{
  memset(strrecvbuffer,0,sizeof(strrecvbuffer));
  memset(strsendbuffer,0,sizeof(strsendbuffer));
 
  SPRINTF(strsendbuffer,sizeof(strsendbuffer),"<activetest>ok</activetest>");
  logfile.Write("发送：%s\n",strsendbuffer);
  if (TcpClient.Write(strsendbuffer)==false) return false; // 向服务端发送请求报文。

  if (TcpClient.Read(strrecvbuffer,20)==false) return false; // 接收服务端的回应报文。
  logfile.Write("接收：%s\n",strrecvbuffer);

  return true;
}

void EXIT(int sig)
{
  logfile.Write("收到信号%d,程序退出\n",sig);  
  exit(0);
}


// 登录业务。 
bool Login(const char* argv)    
{
  memset(strrecvbuffer,0,sizeof(strrecvbuffer));
  memset(strsendbuffer,0,sizeof(strsendbuffer));
 
  SPRINTF(strsendbuffer,sizeof(strsendbuffer),"%s<clienttype>1</clienttype>",argv);
  logfile.Write("发送：%s\n",strsendbuffer);
  if (TcpClient.Write(strsendbuffer)==false) return false; // 向服务端发送请求报文。

  if (TcpClient.Read(strrecvbuffer,20)==false) return false; // 接收服务端的回应报文。
  logfile.Write("接收：%s\n",strrecvbuffer);

  logfile.Write("登录(%s,%d)成功。\n",starg.ip,starg.port); 

  return true;
}

bool _tcpputfiles()
{
  //打开本地目录
  CDir Dir;
  if(Dir.OpenDir(starg.clientpath,starg.matchname,10000,starg.andchild) == false)
  {
    logfile.Write("Dir.OpenDir(%s) failed.\n",starg.clientpath);
    return false;
  }

  while(true)
  { 
    memset(strrecvbuffer,0,sizeof(strrecvbuffer));
    memset(strsendbuffer,0,sizeof(strsendbuffer));

    //每次读取一条文件信息
    if(Dir.ReadDir() == false) { break;} 
 
    //把文件名，修改时间，大小传给对端
    SNPRINTF(strsendbuffer,sizeof(strsendbuffer),1000,"<filename>%s</filename><mtime>%s</mtime><size>%d</size>"\
                                                 ,Dir.m_FullFileName,Dir.m_ModifyTime,Dir.m_FileSize);

    logfile.Write("strsendbuffer=%s.\n",strsendbuffer);

    if(TcpClient.Write(strsendbuffer) == false)
    {
       logfile.Write("TcpClient.Write() failed.\n");
    }
      
    //把文件内容传给对端
    
   
    //接收对端的确认报文
    if(TcpClient.Read(strrecvbuffer,20) == false)
    {
       logfile.Write("TcpClient.Read() failed.\n");
       return false;
    }
    
    logfile.Write("strrecvbuffer=%s\n",strrecvbuffer);
    //删除或转存本地的文件


  }
  return true;
}








  

