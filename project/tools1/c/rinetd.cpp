/*反向代理程序-外网端*/
#include "_public.h"
#include <sys/timerfd.h>

CLogFile logfile;
CPActive PActive;

//代理路由参数的结构体
struct st_route
{
  int  listenport;   //监听端口
  char dstip[31];    //目标主机ip
  int  dstport;	     //目标主机端口
  int  listensock;   //监听socket
}stroute;

vector<struct st_route> vroute;       //代理路由的容器
bool loadroute(const char* inifile);  //把代理路由参数加载到容器中

// 初始化服务端的监听端口。
int initserver(int port);

int epollfd=0; //epoll句柄
int tfd=0;     //定时器句柄

#define MAXSOCK 1024
int clientsocks[MAXSOCK]={};  //存放每个socket连接对端的socket的值
int clientatime[MAXSOCK]={};  //存放每个socket连接最后一次收发报文的时间

int cmdlistensock=0;  //服务端监听内网客户端的socket
int cmdconnsock=0;    //内网客户端与服务端的控制通道

void EXIT(int sig);

int main(int argc,char *argv[])
{
  if (argc != 4) 
  {
     printf("\n");
     printf("Using: ./rinetd logfile inifile cmdport\n\n");
     printf("Example: ./rinetd /tmp/rinetd.log /etc/inetd.conf 4000\n\n");
     printf("	      /project/tools1/bin/procctl 5 /project/tools1/bin/rinetd /tmp/rinetd.log /etc/inetd.conf 4000\n\n");

     printf("logfile  程序运行的日志.\n");
     printf("inifile  代理服务参数配置文件，格式: 监听端口 目标ip 目标端口.\n");
     printf("cmdport  与内网代理程序的通讯端口.\n\n");
     return -1;
  }

  //关闭I0和信号，设置本程序的信号
  CloseIOAndSignal();  signal(2,EXIT); signal(15,EXIT);

  if(logfile.Open(argv[1],"a+")==false)
  {
    printf("logfile.Open(%s) failed.\n",argv[1]); return -1;
  }
 
  //加载参数配置文件
  if(loadroute(argv[2]) == false)
  {
    logfile.Write("loadroute(%s) failed.\n",argv[2]); return -1;
  }

  logfile.Write("加载代理路由参数成功(%d)。\n",vroute.size());

  //初始化内网程序的端口，等待内网程序发起连接，建立控制通道
  if( (cmdlistensock=initserver(atoi(argv[3]))) <0 )
  {
    logfile.Write("initserver %d failed.\n",atoi(argv[3])); EXIT(-1);
  }

  //等待内网程序的连接请求，建立控制通道
  struct sockaddr_in client;
  socklen_t len=sizeof(client);
  cmdconnsock = accept(cmdlistensock,(struct sockaddr*)&client,&len);
  if(cmdconnsock < 0)
  {
    logfile.Write("accept() failed.\n"); EXIT(-1);
  }
  logfile.Write("与内部的控制通道已建立(cmdconnsock=%d).\n",cmdconnsock);

  // 初始化服务端用于监听的socket。
  for(unsigned ii=0; ii<vroute.size(); ii++)
  {
     if( (vroute[ii].listensock=initserver(vroute[ii].listenport)) < 0)
     {
        logfile.Write("initserver(%d) failed.\n",vroute[ii].listenport); EXIT(-1);
     }
    //把socket设置为非阻塞模式
    fcntl(vroute[ii].listensock,F_SETFL,fcntl(vroute[ii].listensock,F_GETFL,0)|O_NONBLOCK);
  }

  // 创建epoll句柄。
  epollfd=epoll_create(1);

  struct epoll_event ev;  // 声明事件的数据结构。

  // 为监听的socket准备可读事件。
  for(unsigned ii=0; ii<vroute.size(); ii++) 
  { 
    ev.events=EPOLLIN;      // 读事件。
    ev.data.fd=vroute[ii].listensock;  // 指定事件的自定义数据，会随着epoll_wait()返回的事件一并返回。
    epoll_ctl(epollfd,EPOLL_CTL_ADD,vroute[ii].listensock,&ev); // 把监听的socket的事件加入epollfd中。
  }

  //注意，监听内网程序的cmdlistensock和控制通道cmdconnsock是阻塞的，也不需要epoll管理。
 
  //创建定时器
  tfd=timerfd_create(CLOCK_MONOTONIC,TFD_NONBLOCK|TFD_CLOEXEC);
  
  struct itimerspec timeout;
  memset(&timeout,0,sizeof(struct itimerspec));
  timeout.it_value.tv_sec=20; //超时时间为20秒
  timeout.it_value.tv_nsec=0;
  timerfd_settime(tfd,0,&timeout,NULL); 

  //为定时器准备事件
  ev.data.fd=tfd;
  ev.events=EPOLLIN|EPOLLET;  //读事件，并且一定要ET模式  
  epoll_ctl(epollfd,EPOLL_CTL_ADD,tfd,&ev);

  PActive.AddPInfo(30,"inetd"); //设置进程的超时时间30秒

  struct epoll_event evs[10];      // 存放epoll返回的事件。

  while (true)
  {
    // 等待监视的socket有事件发生。
    int infds=epoll_wait(epollfd,evs,10,-1);

    // 返回失败。
    if (infds < 0)
    {
      logfile.Write("epoll_wait() failed.\n"); break;
    }

    // 如果infds>0，表示有事件发生的socket的数量。
    // 遍历epoll返回的已发生事件的数组evs。
    for (int ii=0;ii<infds;ii++)
    {
      //logfile.Write("events=%d,data.fd=%d\n",evs[ii].events,evs[ii].data.fd);
      /////////////////////////////////////////
      //如果定时器的时间已到,向控制通道发送心跳,设置进程心跳,清理空闲的客户端socket
      if(evs[ii].data.fd == tfd)
      {
        timerfd_settime(tfd,0,&timeout,NULL); //重新设置定时器
  
        PActive.UptATime();
  
        //通过控制通道向内网程序发送心跳  
	char buffer[256]={};
        strcpy(buffer,"<activetest>");
        if(send(cmdconnsock,buffer,strlen(buffer),0) <= 0)
        {
 	  logfile.Write("与内网程序的控制通道已断开.\n"); EXIT(-1);
  	}  
 
        for(int kk=0; kk<MAXSOCK; kk++)
        {
          if( clientsocks[kk]>0 && (time(0)-clientatime[kk])>80 )
          {
            logfile.Write("client(%d,%d) timeout.\n",clientsocks[kk],clientsocks[clientsocks[kk]]);
            close(clientsocks[kk]);
            close(clientsocks[clientsocks[kk]]);
            clientsocks[clientsocks[kk]] = 0; 
  	    clientsocks[kk] = 0;
          }           
        }
        continue;
      }
      //////////////////////////////////////////////////////////////
      // 如果发生事件的是监听外网listensock，表示外网有新的客户端连接上来。
      unsigned jj;
      for(jj=0; jj<vroute.size(); jj++)
      {
        if (evs[ii].data.fd==vroute[jj].listensock)
        {
          //接收客户端的连接
          struct sockaddr_in client;
          socklen_t len = sizeof(client);
          int srcsock = accept(vroute[jj].listensock,(struct sockaddr*)&client,&len);
          if(srcsock < 0) { break;}
          if(srcsock >= MAXSOCK)
          {
	    logfile.Write("连接数已超过最大值%d.\n",MAXSOCK); close(srcsock); break;
	  }

          //通过控制通道向内网程序发送命令，把路由参数传给它
          char buffer[256];
	  memset(buffer,0,sizeof(buffer));
	  sprintf(buffer,"<dstip>%s</dstip><dstport>%d</dstport>",vroute[jj].dstip,vroute[jj].dstport);
          if(send(cmdconnsock,buffer,strlen(buffer),0) < 0)
          {
 	    logfile.Write("与内网的控制通道已断开.\n"); EXIT(-1);
  	  }

          //接收内网的连接
          int dstsock=accept(cmdlistensock,(struct sockaddr*)&client,&len);
          if(dstsock < 0) { close(srcsock); break;}
          if(dstsock >= MAXSOCK)
          {
            logfile.Write("连接数已超过最大值%d.\n",MAXSOCK); close(srcsock); close(dstsock); break;
          }

          //把外网和内网客户端对接起来
          //为新客户端准备可读事件，并添加到epoll中。
          ev.data.fd=srcsock;
          ev.events=EPOLLIN;
          epoll_ctl(epollfd,EPOLL_CTL_ADD,srcsock,&ev);
          ev.data.fd=dstsock;
          ev.events=EPOLLIN;
          ev.data.fd=dstsock;
          epoll_ctl(epollfd,EPOLL_CTL_ADD,dstsock,&ev);
  
          //更新clientsock数组两端socket的值和时间
          clientsocks[srcsock]=dstsock;
          clientsocks[dstsock]=srcsock;
          clientatime[srcsock]=time(0);
          clientatime[dstsock]=time(0);
          
  	  logfile.Write("accept port %d client(%d,%d) ok.\n",vroute[jj].listenport,srcsock,dstsock);
          break;
        }
      }
 
      //如果jj< vroute.size(),说明上面的事件已经被处理
      if(jj < vroute.size()) {continue;}

      //以下流程处理内外网链路socket的事件
      char buffer[5000]; // 存放从客户端读取的数据。
      int  buflen=0;    //从socket读取到的数据的大小
      memset(buffer,0,sizeof(buffer));
      if ( (buflen=recv(evs[ii].data.fd,buffer,sizeof(buffer),0)) <=0)
      {
        // 如果连接已断开。
        logfile.Write("client(%d,%d) disconnected.\n",evs[ii].data.fd,clientsocks[evs[ii].data.fd]);
        close(clientsocks[evs[ii].data.fd]);            // 关闭对端的socket
        close(evs[ii].data.fd);            // 关闭客户端的socket
        clientsocks[clientsocks[evs[ii].data.fd]]=0;  //两行顺序不能反
        clientsocks[evs[ii].data.fd] = 0;
        continue;
      }

      // 如果有报文发过来。
      //logfile.Write("from %d to %d, %dbytes.\n",evs[ii].data.fd,clientsocks[evs[ii].data.fd],buflen);
      // 把接收到的报文内容原封不动的发回去。
      struct pollfd pfd;
      pfd.fd=clientsocks[evs[ii].data.fd];
      pfd.events=POLLOUT;
      int infds=poll(&pfd,1,5000);
      if(infds == 0) { continue;  }  //5秒后socket不可写，那就放弃这次的数据
      send(clientsocks[evs[ii].data.fd],buffer,buflen,0);

      //更新socket两端连接使用时间
      clientatime[evs[ii].data.fd]=time(0);
      clientatime[clientsocks[evs[ii].data.fd]]=time(0);
    }
  }

  return 0;
}

void EXIT(int sig)
{
  logfile.Write("收到信号%d,程序退出.\n",sig);
  
  close(cmdconnsock);
  close(cmdlistensock);

  for(unsigned ii=0; ii<vroute.size(); ii++)
  {
    close(vroute[ii].listensock);
  }

  for(unsigned ii=0; ii<MAXSOCK; ii++)
  {
    if(clientsocks[ii]>0) close(clientsocks[ii]);
  }
  
  close(epollfd);
  close(tfd);

  exit(0);
}

// 初始化服务端的监听端口。
int initserver(int port)
{
  int sock = socket(AF_INET,SOCK_STREAM,0);
  if (sock < 0)
  {
    perror("socket() failed"); return -1;
  }

  int opt = 1; unsigned int len = sizeof(opt);
  setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,&opt,len);

  struct sockaddr_in servaddr;
  servaddr.sin_family = AF_INET;
  servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
  servaddr.sin_port = htons(port);

  if (bind(sock,(struct sockaddr *)&servaddr,sizeof(servaddr)) < 0 )
  {
    perror("bind() failed"); close(sock); return -1;
  }

  if (listen(sock,5) != 0 )
  {
    perror("listen() failed"); close(sock); return -1;
  }

  return sock;
}

//把代理路由参数加载到容器中
bool loadroute(const char* inifile)
{
  CFile File;
  if(File.Open(inifile,"r")==false)
  {
    logfile.Write("open(%s) failed.\n",inifile); return false;
  }

  char buffer[256];
  CCmdStr CmdStr;  

  while(true)
  {
    memset(buffer,0,sizeof(buffer));
    if(File.FFGETS(buffer,sizeof(buffer)-1)==false) {break; }

    char* pos=strstr(buffer,"#");
    if(pos != NULL) { *pos='\0';}
    DeleteRChar(buffer,' ');
    DeleteLChar(buffer,' ');
    UpdateStr(buffer,"  "," ",true);

    CmdStr.SplitToCmd(buffer," ");
    if(CmdStr.CmdCount() != 3) {continue;}
    
    memset(&stroute,0,sizeof(struct st_route));
    CmdStr.GetValue(0,&stroute.listenport);   
    CmdStr.GetValue(1,stroute.dstip,30);   
    CmdStr.GetValue(2,&stroute.dstport);
  
    vroute.push_back(stroute);   
  }
  return true;
}


