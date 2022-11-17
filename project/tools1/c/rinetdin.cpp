/*网路反向代理服务程序-内网端*/
#include "_public.h"
#include <sys/timerfd.h>

CLogFile logfile;
CPActive PActive;

int cmdconnsock=0; //控制通道的sock
int epollfd=0; //epoll句柄
int tfd=0;     //定时器句柄

#define MAXSOCK 1024
int clientsocks[MAXSOCK]={};  //存放每个socket连接对端的socket的值
int clientatime[MAXSOCK]={};  //存放每个socket连接最后一次收发报文的时间

//向目标ip和端口发起socket连接
int conntodst( const char* ip, const int port);                

void EXIT(int sig);

int main(int argc,char *argv[])
{
  if (argc != 4) 
  {
     printf("\n");
     printf("Using: ./rinetdin logfile ip port\n\n");
     printf("Example: ./rinetdin /tmp/rinetdin.log 101.34.83.118 4000\n\n");
     printf("	      /project/tools1/bin/procctl 5 /project/tools1/bin/rinetdin /tmp/rinetdin.log 101.34.83.118 4000\n\n");

     printf("logfile 本程序运行的日志.\n");
     printf("ip      外网代理服务端的地址.\n");
     printf("port    外网代理服务端的端口.\n\n");
     return -1;
  }

  //关闭I0和信号，设置本程序的信号
  CloseIOAndSignal();  signal(2,EXIT); signal(15,EXIT);

  if(logfile.Open(argv[1],"a+")==false)
  {
    printf("logfile.Open(%s) failed.\n",argv[1]); return -1;
  }
  
  //建立内网程序与外网程序的控制通道
  CTcpClient TcpClient;
  if(TcpClient.ConnectToServer(argv[2],atoi(argv[3]))==false)
  {
    logfile.Write("TcpClient.ConnToServer(%s,%d) failed.\n",argv[2],atoi(argv[3])); EXIT(-1);
  }
 
  cmdconnsock=TcpClient.m_connfd;
  fcntl(cmdconnsock,F_SETFL,fcntl(cmdconnsock,F_GETFD,0)|O_NONBLOCK);
  logfile.Write("与外部的控制通道已建立(cmdconnsock=%d).\n",cmdconnsock);
  
  // 创建epoll句柄。
  epollfd=epoll_create(1);

  struct epoll_event ev;  // 声明事件的数据结构。

  //为控制通道准备可读事件
  ev.events=EPOLLIN;
  ev.data.fd=cmdconnsock;
  epoll_ctl(epollfd,EPOLL_CTL_ADD,cmdconnsock,&ev);

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
      perror("epoll() failed"); break;
    }

    // 如果infds>0，表示有事件发生的socket的数量。
    // 遍历epoll返回的已发生事件的数组evs。
    for (int ii=0;ii<infds;ii++)
    {
      //logfile.Write("events=%d,data.fd=%d\n",evs[ii].events,evs[ii].data.fd);
      /////////////////////////////////////////
      //如果定时器的时间已到,设置进程心跳,清理空闲的客户端socket
      if(evs[ii].data.fd == tfd)
      {
        timerfd_settime(tfd,0,&timeout,NULL); //重新设置定时器
  
        PActive.UptATime();
  
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
      //如果发生事件的是控制通道
      if(evs[ii].data.fd == cmdconnsock) 
      {
         //读取控制报文内容
         char buffer[256];
	 memset(buffer,0,sizeof(buffer));
	 if(recv(cmdconnsock,buffer,200,0) < 0 )
         {
 	   logfile.Write("与外网的控制通道已断开.\n"); EXIT(-1);
	 }
       
	 //如果收到的是心跳报文          
         if( strcmp(buffer,"<activetest>") == 0) {continue;}

	 //如果是收到新建连接的命令，执行以下流程
   	 //向外网服务端发起连接请求
         int srcsock=conntodst(argv[2],atoi(argv[3]));
         if(srcsock < 0) { continue; }
         if(srcsock >= MAXSOCK)
         {
           logfile.Write("连接数已超过最大值%d.\n",MAXSOCK); close(srcsock); continue;
         } 

 	 //从控制报文中获取目标地址和端口
         char dstip[31]; int dstport;
         GetXMLBuffer(buffer,"dstip",dstip,30);    
         GetXMLBuffer(buffer,"dstport",&dstport);

         //向目标服务地址和端口发起socket连接
         int dstsock=conntodst(dstip,dstport);
         if(dstsock < 0) { continue; }
         if(dstsock >= MAXSOCK)
         {
           logfile.Write("连接数已超过最大值%d.\n",MAXSOCK); close(srcsock); close(dstsock); continue;
         } 
         logfile.Write("新建内外网通道(%d,%d) ok.\n",srcsock,dstsock);
         
         //把内网和外网sock对接到一起
         ev.data.fd=srcsock;
         ev.events=EPOLLIN;
         epoll_ctl(epollfd,EPOLL_CTL_ADD,srcsock,&ev);
         ev.data.fd=dstsock;
         ev.events=EPOLLIN;
         epoll_ctl(epollfd,EPOLL_CTL_ADD,dstsock,&ev);
         
         //更新clientsock数组两端socket的值和时间
         clientsocks[srcsock]=dstsock;
         clientsocks[dstsock]=srcsock;
         clientatime[srcsock]=time(0);
         clientatime[dstsock]=time(0);

	 continue;
      }

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

      //更新两端socket连接使用时间
      clientatime[evs[ii].data.fd]=time(0);
      clientatime[clientsocks[evs[ii].data.fd]]=time(0);
    }
  }

  return 0;
}

void EXIT(int sig)
{
  logfile.Write("收到信号%d,程序退出.\n",sig);
  
  for(unsigned ii=0; ii<MAXSOCK; ii++)
  {
    if(clientsocks[ii]>0) close(clientsocks[ii]);
  }
  
  close(epollfd);
  close(tfd);

  exit(0);
}

//向目标ip和端口发起socket连接
int conntodst( const char* ip, const int port)                
{
  // 第1步：创建客户端的socket。
  int sockfd;
  if ( (sockfd = socket(AF_INET,SOCK_STREAM,0))==-1) { return -1; }

  // 第2步：向服务器发起连接请求。
  struct hostent* h;
  if ( (h = gethostbyname(ip)) == 0 )   // 指定服务端的ip地址。
  {  close(sockfd); return -1; }

  struct sockaddr_in servaddr;
  memset(&servaddr,0,sizeof(servaddr));
  servaddr.sin_family = AF_INET;
  servaddr.sin_port = htons(port); // 指定服务端的通讯端口。
  memcpy(&servaddr.sin_addr,h->h_addr,h->h_length);

  fcntl(sockfd,F_SETFL,fcntl(sockfd,F_GETFD,0)|O_NONBLOCK); 
 
  connect(sockfd,(struct sockaddr *)&servaddr,sizeof(servaddr));

  return sockfd;
}



