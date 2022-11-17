/*
 * 程序名：tcpgetfiles.cpp，文件下载的客户端
*/
#include "_public.h"

bool Login(const char* argv);    // 登录业务。
void EXIT(int sig); //信号2和15的处理函数
void _help();
bool _xmltoarg(char* strxmlbuffer); 
void _tcpgetfiles();
bool RecvFile(const int sockfd, const char* filename,const char* mtime, int filesize);

struct st_arg
{
  int clienttype;          //服务方式 1-文件上传， 2-文件下载
  char ip[31];             //服务端ip
  int port;                //服务端端口
  int ptype;               //文件下载成功后文件的处理方式，1-删除文件，2-移动到备份目录
  char srvpath[301];    //服务端文件存放的根目录
  char srvpathbak[301]; //服务端文件下载成功后，服务端文件的备份目录，当ptype==2时有效
  bool andchild;           //是否下载srvpath目录下各级子目录的文件
  char matchname[301];     //待下载文件名的匹配规则，如"*.txt,*.xml"
  char clientpath[301];       //本地文件存放的根目录
  int timevl;             //扫描服务端目录文件的时间间隔，单位：秒
  int timeout;             //进程心跳的超时时间
  char pname[51];          //进程名，建议用"tcpgetfiles_后缀"的方式
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

  CloseIOAndSignal(); signal(2,EXIT); signal(15,EXIT);

  if(logfile.Open(argv[1],"a+") == false)
  {
     printf("打开日志文件失败\n"); return -1;
  }

  if(_xmltoarg(argv[2]) == false) { logfile.Write("解析xml失败\n"); return -1; }

  PActive.AddPInfo(starg.timeout,starg.pname);  //把进程的心跳信息写入共享内存

  // 向服务端发起连接请求。
  if (TcpClient.ConnectToServer(starg.ip,starg.port)==false)
  {
    logfile.Write("TcpClient.ConnectToServer(%s,%d) failed.\n",starg.ip,starg.port); EXIT(-1);
  }

  // 登录业务。
  if (Login(argv[2])==false) { logfile.Write("Login() failed.\n"); EXIT(-1); }

  _tcpgetfiles() ;     
 
  EXIT(0);
}

void _help()
{
  putchar('\n');
  printf("Using:/project/tools1/bin/tcpgetfiles logfilename xmlbuffer\n\n");
  printf("Example:/project/tools1/bin/procctl 20 /project/tools1/bin/tcpgetfiles /tmp/tcpgetfiles.log \"<ip>101.34.83.118</ip><port>5005</port><ptype>1</ptype><srvpath>/tmp/tcp/surfdata2</srvpath><andchild>true</andchild><matchname>*.xml,*.csv</matchname><clientpath>/tmp/tcp/surfdata1</clientpath><timevl>10</timevl><timeout>50</timeout><pname>tcpgetfiles_surfdata</pname>\" \n\n");

  printf("Example:/project/tools1/bin/procctl 20 /project/tools1/bin/tcpgetfiles /tmp/tcpgetfiles.log \"<ip>47.107.41.197</ip><port>5005</port><ptype>2</ptype><srvpath>/tmp/tcp/surfdata2</srvpath><srvpathbak>/tmp/tcp/surfdata2bak</srvpathbak><andchild>true</andchild><matchname>*.xml,*.csv</matchname><clientpath>/tmp/tcp/surfdata1</clientpath><timevl>10</timevl><timeout>50</timeout><pname>tcpgetfiles_surfdata</pname>\" \n\n\n");

  printf("本程序是通用的功能模块,采用tcp协议把文件发送给服务端。\n");
  printf("logfilename 是本程序运行的日志文件。\n");
  printf("xmlbuffer为文件下载的参数，如下：\n");
  printf("<ip>47.107.41.197:21</ip> 服务端ip\n");
  printf("<port>5005</port> 服务端端口"); 
  printf("<ptype>1</ptype> 文件下载成功后文件的处理方式，1-删除文件，2-移动到备份目录\n");
  printf("<srvpath>/tmp/tcp/surfdata2</srvpath> 服务端文件存放的根目录\n");
  printf("<srvpathbak>/tmp/tcp/surfdata2bak</srvpathbak> 服务端文件下载成功后，服务端文件的备份目录，当ptype==2时有效\n");
  printf("<andchild>true</andchild> 否下载srvpath目录下各级子目录的文件\n"); 
  printf("<matchname>*.xml,*.csv</matchname> 待下载文件的匹配规则。"\
           "不匹配的文件不会被下载，本字段尽可能设置精确，不建议用*匹配全部的文件\n");
  printf("<clientpath>/tmp/tcp/surfdata1</clientpath> 本地文件存放的根目录\n");
  printf("<timevl>10</timevl> 扫描服务端目录文件的时间间隔，单位：秒,取值在1-30之间\n");
  printf("<timeout>50</timeout> 进程心跳的超时时间，视文件大小和网络带宽而定，建议设置50以上。\n");
  printf("<pname>tcpgetfiles_surfdata</pname> 进程名，建议用\"tcpgetfiles_后缀\"的方式\n\n\n");
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

  GetXMLBuffer(strxmlbuffer,"srvpath",starg.srvpath);
  if(strlen(starg.srvpath) == 0) { logfile.Write("srvpath is null \n"); return false;}
 
  GetXMLBuffer(strxmlbuffer,"srvpathbak",starg.srvpathbak);
  if(starg.ptype==2 && (strlen(starg.srvpathbak)==0) ) { logfile.Write("srvpathbak is null \n"); return false;}

  GetXMLBuffer(strxmlbuffer,"andchild",&starg.andchild);

  GetXMLBuffer(strxmlbuffer,"matchname",starg.matchname);
  if(strlen(starg.matchname) == 0) { logfile.Write("matchname is null \n"); return false;} 

  GetXMLBuffer(strxmlbuffer,"clientpath",starg.clientpath);
  if(strlen(starg.clientpath) == 0) { logfile.Write("clientpath is null \n"); return false;}  

  GetXMLBuffer(strxmlbuffer,"timevl",&starg.timevl);
  if(starg.timevl <1) { logfile.Write("timevl is too small"); return false;} 
  if(starg.timevl > 30) { starg.timevl = 30;}

  GetXMLBuffer(strxmlbuffer,"timeout",&starg.timeout);
  if(starg.timeout==0) { logfile.Write("timeout is null"); return false;}
  if(starg.timeout < 50) { starg.timeout = 50 ; }

  GetXMLBuffer(strxmlbuffer,"pname",starg.pname,50);
  if(strlen(starg.pname) == 0) { logfile.Write("pname is null \n"); return false;}  

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
 
  SPRINTF(strsendbuffer,sizeof(strsendbuffer),"%s<clienttype>2</clienttype>",argv);
  logfile.Write("发送：%s\n",strsendbuffer);
  if (TcpClient.Write(strsendbuffer)==false) return false; // 向服务端发送请求报文。

  if (TcpClient.Read(strrecvbuffer,20)==false) return false; // 接收服务端的回应报文。
  logfile.Write("接收：%s\n",strrecvbuffer);

  logfile.Write("登录(%s,%d)成功。\n",starg.ip,starg.port); 

  return true;
}

void _tcpgetfiles()
{
  PActive.AddPInfo(starg.timeout,starg.pname);

  while(true)
  {
    memset(strrecvbuffer,0,sizeof(strrecvbuffer));
    memset(strsendbuffer,0,sizeof(strsendbuffer));
 
    PActive.UptATime();
    
    if(TcpClient.Read(strrecvbuffer,starg.timevl+10) == false) 
    {
      logfile.Write("TcpClient.Read() failed.\n");  return;
    }
    //logfile.Write("strrecvbuffer=%s.\n",strrecvbuffer); 
  
    //处理心跳报文
    if( strcmp(strrecvbuffer,"<activetest>ok</activetest>") == 0)
    {
      strcpy(strsendbuffer,"ok");
      logfile.Write("strsendbuffer=%s.\n",strsendbuffer); 
      if(TcpClient.Write(strsendbuffer)==false)
      {
        logfile.Write("TcpClient.Write() failed.\n"); return;
      }  
    }

    //处理上传文件的请求报文
    if(strncmp(strrecvbuffer,"<filename>",10) == 0)
    {
      //解析xml
      char serverfilename[301]; memset(serverfilename,0,sizeof(serverfilename));
      char mtime[21];           memset(mtime,0,sizeof(mtime));
      int filesize=0;  
 
      GetXMLBuffer(strrecvbuffer,"filename",serverfilename,300);
      GetXMLBuffer(strrecvbuffer,"mtime",mtime,19);
      GetXMLBuffer(strrecvbuffer,"size",&filesize);

      
      //接收文件的内容
      //客户端和服务端的存放目录是不一样的，这里把文件全名中的srvpath替换成clientpath，第三个参数为false
      char clientfilename[301] ;  memset(clientfilename,0,sizeof(clientfilename));
      strcpy(clientfilename,serverfilename);
      UpdateStr(clientfilename,starg.srvpath,starg.clientpath,false);
      
      logfile.Write("recv %s(%d) ...",clientfilename,filesize);   

      if(RecvFile(TcpClient.m_connfd,clientfilename,mtime,filesize) == true)
      { 
         logfile.WriteEx("ok.\n");
         SNPRINTF(strsendbuffer,sizeof(strsendbuffer),1000,"<filename>%s</filename><result>ok</result>",serverfilename);
      }
      else
      { 
         logfile.WriteEx("failed.\n");
         SNPRINTF(strsendbuffer,sizeof(strsendbuffer),1000,"<filename>%s</filename><result>failed</result>",serverfilename);
      }
      
      //发送确认报文
     // logfile.Write("strsendbuffer=%s.\n",strsendbuffer);
      if(TcpClient.Write(strsendbuffer)==false)
      {
        logfile.Write("TcpClient.Write() failed.\n"); return;
      }
    }   
  }
  return ;
}

//接收文件的内容
bool RecvFile(const int sockfd, const char* filename,const char* mtime, int filesize)
{
  //生成临时文件名
  char strfilenametmp[301];
  SNPRINTF(strfilenametmp,sizeof(strfilenametmp),300,"%s.tmp",filename);

  int totalbytes=0 ;    //已接收文件的总字节数
  int onread=0;         //本次打算接收的字节数
  char buffer[1000];    //接收文件内容的缓冲区
  FILE* fp = NULL;
  //创建临时文件
  if((fp=FOPEN(strfilenametmp,"wb")) == NULL ) {return false;}
 
  while(true)
  {
    memset(buffer,0,sizeof(buffer));
    //计算每次接收的字节数
    if(filesize-totalbytes > 1000) { onread=1000; }
    else
    { onread = filesize-totalbytes; }

    //接收文件内容
    if( Readn(sockfd,buffer,onread) == false) {fclose(fp); return false;}
    
    //把接收的内容写入文件
    fwrite(buffer,1,onread,fp);

    //计算已接收的字节数，如果接收完，跳出循环
    totalbytes += onread;
    if(totalbytes == filesize) {break;}
  }
  //关闭临时文件
  fclose(fp);

  //重置文件的时间
  UTime(strfilenametmp,mtime);

  //把临时文件改为正式文件
  if(RENAME(strfilenametmp,filename) == false) {return false;}  
  
  return true;
}



