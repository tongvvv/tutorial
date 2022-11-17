/*
 * 程序名：fileserver.cpp，文件传输的服务端
*/
#include "_public.h"

CLogFile logfile;      // 服务程序的运行日志。
CTcpServer TcpServer;  // 创建服务端对象。
CPActive PActive;

struct st_arg
{
  int clienttype;          //客户端类型 1-文件上传， 2-文件下载
  char ip[31];             //服务端ip
  int port;                //服务端端口
  int ptype;               //文件传输成功后文件的处理方式，1-删除文件，2-移动到备份目录
  char clientpath[301];    //本地文件存放的根目录
  bool andchild;           //是否传输各级子目录的文件
  char matchname[301];     //文件名的匹配规则，如"*.txt,*.xml"
  char srvpath[301];       //服务端文件存放的根目录
  char srvpathbak[301];    //服务端文件存放的备份目录，ptype==2时有效
  int timevl;              //扫描目录文件的时间间隔，单位：秒
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

//接收文件上传的内容
bool RecvFile(const int sockfd, const char* filename,const char* mtime, int filesize);

//下载文件的主函数
void SendFilesMain();

//注意，SendFilesMain函数调用了_tcpputfiles,AckMessage和ActiveTest函数，而_tcpputfiles调用了SendFile

bool ActiveTest();

bool _tcpputfiles();

//发送文件下载的内容给客户端
bool SendFile(const int sockfd,const char* filename,const int filesize);

bool AckMessage(const char* strrecvbuffer);

bool bcontinue = true;  //如果调用 _tcpputfiles发送了文件，bcontinue 为 true ，初始化为true

int main(int argc,char *argv[])
{
  if (argc!=3)
  {
    printf("Using:./fileserver port logfile \nExample:./fileserver 5005 /tmp/fileserver.log \n\n"); 
    printf("Example:/project/tools1/bin/procctl 10 /project/tools1/bin/fileserver 5005 /log/idc/fileserver.log\n\n");
    return -1;
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

    
    if (fork()>0) { TcpServer.CloseClient(); continue; }  // 父进程继续回到Accept()。
   
    // 子进程重新设置退出信号。
    signal(SIGINT,ChldEXIT); signal(SIGTERM,ChldEXIT);

    TcpServer.CloseListen();
    
   
    //子进程与客户端通信
    
    //处理登录客户端的登录报文
    if(ClientLogin() == false) ChldEXIT(-1);    
    //如果clienttype==1,调用上传文件主函数
    if(starg.clienttype==1) { RecvFilesMain();}
    //如果clienttype==2,调用下载文件主函数
    if(starg.clienttype==2) { SendFilesMain();}
    

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

  logfile.Write("%s login %s\n",TcpServer.GetIP(),strsendbuffer);

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
  GetXMLBuffer(strxmlbuffer,"srvpathbak",starg.srvpathbak);

  GetXMLBuffer(strxmlbuffer,"timevl",&starg.timevl);
  if(starg.timevl ==0 ) { logfile.Write("timevl is null\n"); return false;}
  if(starg.timevl > 30) { starg.timevl = 30;}

  GetXMLBuffer(strxmlbuffer,"timeout",&starg.timeout);
  if(starg.timeout==0) { logfile.Write("timeout is null\n"); return false;}
  if(starg.timeout < 50) { starg.timeout = 50 ; }

  GetXMLBuffer(strxmlbuffer,"pname",starg.pname,50);
  strcat(starg.pname,"_srv");  

  return true;
}


//上传文件的主函数
void RecvFilesMain()
{
  PActive.AddPInfo(starg.timeout,starg.pname);

  while(true)
  {
    memset(strrecvbuffer,0,sizeof(strrecvbuffer));
    memset(strsendbuffer,0,sizeof(strsendbuffer));
 
    PActive.UptATime();
    //客户端每timevl扫描一次本地目录，这里只要比timevl大几秒就行了
    if(TcpServer.Read(strrecvbuffer,starg.timevl+10) == false) 
    {
      logfile.Write("TcpServer.Read() failed.\n");  return;
    }
    //logfile.Write("strrecvbuffer=%s.\n",strrecvbuffer); 
  
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
    if(strncmp(strrecvbuffer,"<filename>",10) == 0)
    {
      //解析xml
      char clientfilename[301]; memset(clientfilename,0,sizeof(clientfilename));
      char mtime[21];           memset(mtime,0,sizeof(mtime));
      int filesize=0;  
 
      GetXMLBuffer(strrecvbuffer,"filename",clientfilename,300);
      GetXMLBuffer(strrecvbuffer,"mtime",mtime,19);
      GetXMLBuffer(strrecvbuffer,"size",&filesize);

      
      //接收文件的内容
      //客户端和服务端的存放目录是不一样的，这里把文件全名中的clientpath替换成srvpath，第三个参数为false
      char serverfilename[301] ;  memset(serverfilename,0,sizeof(serverfilename));
      strcpy(serverfilename,clientfilename);
      UpdateStr(serverfilename,starg.clientpath,starg.srvpath,false);
      
      logfile.Write("recv %s(%d) ...",serverfilename,filesize);   

      if(RecvFile(TcpServer.m_connfd,serverfilename,mtime,filesize) == true)
      { 
         logfile.WriteEx("ok.\n");
         SNPRINTF(strsendbuffer,sizeof(strsendbuffer),1000,"<filename>%s</filename><result>ok</result>",clientfilename);
      }
      else
      { 
         logfile.WriteEx("failed.\n");
         SNPRINTF(strsendbuffer,sizeof(strsendbuffer),1000,"<filename>%s</filename><result>failed</result>",clientfilename);
      }
      
      //发送确认报文
     //logfile.Write("strsendbuffer=%s.\n",strsendbuffer);
      if(TcpServer.Write(strsendbuffer)==false)
      {
        logfile.Write("TcpServer.Write() failed.\n"); return;
      }
    }   
  }//while(true) 
}

//接收文件上传的内容
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

//下载文件的主函数
void SendFilesMain()
{
  PActive.AddPInfo(starg.timeout,starg.pname);

  while(true)
  {
    if(_tcpputfiles() == false) { logfile.Write("_tcpputfiles() == false\n"); return; }

    if(bcontinue == false)
    {
      sleep(starg.timevl);
      if(ActiveTest() == false) { logfile.Write("ActiveTest() == false\n"); break; }
    }
    PActive.UptATime();
  } 
}

bool ActiveTest()
{
  memset(strrecvbuffer,0,sizeof(strrecvbuffer));
  memset(strsendbuffer,0,sizeof(strsendbuffer));

  SPRINTF(strsendbuffer,sizeof(strsendbuffer),"<activetest>ok</activetest>");
  logfile.Write("发送：%s\n",strsendbuffer);
  if (TcpServer.Write(strsendbuffer)==false) return false; // 向服务端发送请求报文。

  if (TcpServer.Read(strrecvbuffer,20)==false) return false; // 接收服务端的回应报文。
  logfile.Write("接收：%s\n",strrecvbuffer);

  return true;
} 

bool SendFile(const int sockfd,const char* filename,const int filesize)
{
  int onread=0;      //每次调用fread读取的字节数
  int bytes=0;       //调用fread成功读取的字节数
  char buffer[1000]; //存放读取数据的buffer
  int totalbytes=0;  //从文件中已经读取的总字节数
  FILE* fp=NULL;
  
  if((fp =fopen(filename,"rb"))==NULL) { logfile.Write("fopen failed.\n");return false;  }
  
  while(true)
  {  
     memset(buffer,0,sizeof(buffer));
     
     if(filesize-totalbytes > 1000) { onread=1000; }
     else
     { onread = filesize-totalbytes; }
     
     bytes=fread(buffer,1,onread,fp);
     
     if (bytes>0)
     {  
        if (Writen(sockfd,buffer,bytes)==false) { logfile.Write("Writen() failed.\n");fclose(fp); return false; }
     }                   
                         
     totalbytes += bytes;
     
     if(totalbytes == filesize) { break;}
  }
  fclose(fp);
  return true;
}

bool _tcpputfiles()
{
  CDir Dir;
  if(Dir.OpenDir(starg.srvpath,starg.matchname,10000,starg.andchild) == false)
  { 
    logfile.Write("Dir.OpenDir(%s) failed.\n",starg.clientpath);
    return false;
  }

  int delayed=0; //未收到对端确认报文的文件数量
  int ibuflen=0; //存放strrecvbuffer的长度    

  bcontinue = false;
  while(true)
  {
    memset(strrecvbuffer,0,sizeof(strrecvbuffer));
    memset(strsendbuffer,0,sizeof(strsendbuffer));

    if(Dir.ReadDir() == false) { break;}

    bcontinue = true;
    SNPRINTF(strsendbuffer,sizeof(strsendbuffer),1000,"<filename>%s</filename><mtime>%s</mtime><size>%d</size>"\
                                                 ,Dir.m_FullFileName,Dir.m_ModifyTime,Dir.m_FileSize);

    //logfile.Write("strsendbuffer=%s\n",strsendbuffer);
    if(TcpServer.Write(strsendbuffer) == false)
    {  
       logfile.Write("TcpServer.Write() failed\n"); return false;
    }

    logfile.Write("send %s(%d) ...",Dir.m_FullFileName,Dir.m_FileSize);

    if(SendFile(TcpServer.m_connfd,Dir.m_FullFileName,Dir.m_FileSize) == true)
    { logfile.WriteEx("ok.\n"); delayed++; }
    else
    { logfile.WriteEx("failed.\n"); TcpServer.CloseClient(); return false;}

    PActive.UptATime();
    while(delayed>0)
    {
      memset(strrecvbuffer,0,sizeof(strrecvbuffer));
      if(TcpRead(TcpServer.m_connfd,strrecvbuffer,&ibuflen,-1) == false)
      {
        break;
      }
      delayed--;
      //logfile.Write("strrecvbuffer=%s\n",strrecvbuffer);
      AckMessage(strrecvbuffer);
    }
    PActive.UptATime();
  }//while(true)

  while(delayed>0)
  {
    memset(strrecvbuffer,0,sizeof(strrecvbuffer));
    if(TcpRead(TcpServer.m_connfd,strrecvbuffer,&ibuflen,10) == false)
    {
      break;
    }
    delayed--;
    AckMessage(strrecvbuffer);
  }
  PActive.UptATime();
  return true;
}

bool AckMessage(const char* strrecvbuffer)
{
   char filename[301];
   char result[11];

   memset(filename,0,sizeof(filename));
   memset(result,0,sizeof(result));

   GetXMLBuffer(strrecvbuffer,"filename",filename,300);
   GetXMLBuffer(strrecvbuffer,"result",result,10);

   if(strcmp(result,"ok") != 0 ) {return true;}

   if(starg.ptype == 1)
   {
     if(REMOVE(filename) == false)
     {
       logfile.Write("REMOVE(%s) failed.\n",filename); return false;
     }
   }

   if(starg.ptype == 2)
   {
     char bakfilename[301]; memset(bakfilename,0,sizeof(bakfilename));
     STRCPY(bakfilename,sizeof(bakfilename),filename);
     UpdateStr(bakfilename,starg.srvpath,starg.srvpathbak,false);
     if(RENAME(filename,bakfilename)==false)
     { logfile.Write("RENAME(%s,%s) failed.\n",filename,bakfilename); return false;}
   }
  return true;
}





