#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/fcntl.h>

// 初始化服务端的监听端口。
int initserver(int port);

int main(int argc,char *argv[])
{
  if (argc != 2) { printf("usage: ./tcpselect port\n"); return -1; }

  // 初始化服务端用于监听的socket。
  int listensock = initserver(atoi(argv[1]));
  printf("listensock=%d\n",listensock);

  if (listensock < 0) { printf("initserver() failed.\n"); return -1; }

  //fd_set是一个1024的位图
  fd_set readfds;
  FD_ZERO(&readfds);
  FD_SET(listensock,&readfds);
  int maxfd=listensock;  

  //select的缺点:
  //支持的连接数太少,1024
  //每次调用select都要把fd_set从用户态拷贝到内核，调用之后从内核拷贝到用户态
  //select()返回后需要遍历bitmap，效率较低
  
  //selec水平触发机制：
  //如果事件和数据已经在缓冲区里，那么select()会报告时间，事件不会丢失
  //如果select()已经报告了事件，但程序没有处理它，下次调用select()会重新报告；
  while (true)
  {
    // 事件：1)新客户端的连接请求accept；2)客户端有报文到达recv，可以读；3)客户端连接已断开；
    //       4)可以向客户端发送报文send，可以写。
    // 可读事件  可写事件
    // select() 等待事件的发生(监视哪些socket发生了事件)。
 
    fd_set tmpfds=readfds;
    struct timeval timeout; timeout.tv_sec=10; timeout.tv_usec=0;
    // 当有可读事件发生时，select返回大于0的值，当没有可读事件发生，select阻塞等待
    // 最后一个参数可以设置阻塞等待时间
    int infds=select(maxfd+1,&tmpfds,NULL,NULL,&timeout);    

    if(infds<0) 
    { perror("select() failed.\n"); break;}

    //如果select超时，返回0
    if(infds==0)
    { perror("select() timeout.\n"); continue;}
  

    for(int eventfd=0; eventfd<=maxfd ; eventfd++)
    {
	if(FD_ISSET(eventfd,&tmpfds) <= 0) {continue;}
  
        //发生事件的socket
        if(eventfd == listensock)
        {
           struct sockaddr_in client;
           socklen_t len = sizeof(client);
           int clientsock=accept(listensock,(struct sockaddr*)&client, &len);
           if(clientsock<0) {perror("accept() failed.\n"); continue;}
         
           FD_SET(clientsock,&readfds);
           maxfd = clientsock>maxfd? clientsock:maxfd;
        } 
        else
        {
           //如果客户端有事件，说明有报文发过来或连接已断开
           char buffer[1024] = {};
           
           if(recv(eventfd,buffer,sizeof(buffer),0) <=0) 
           {
 		//连接已断开
 		printf("client(eventfd=%d) disconnect.\n",eventfd);
                close(eventfd);
                FD_CLR(eventfd,&readfds);
                
                //重新计算maxfd的值
                if(eventfd == maxfd)
                {
  		   for(int ii=maxfd; ii>=0; ii--)  //从后往前找
                   {
			if(FD_ISSET(ii,&readfds))
                        { maxfd=ii; break;}
                   }
 		}
           }
           else
           {
		//客户端有报文发过来
	        printf("recv(eventfd=%d) : %s.\n",eventfd,buffer );
                fd_set tmpfds1;
                FD_ZERO(&tmpfds1);
                FD_SET(eventfd,&tmpfds1);
                //如果socket可以写，select立刻返回,如果socket不可以写，select会阻塞等待，直到可写
                //所以这段代码没太大必要。
                if(select(eventfd+1,NULL,&tmpfds1,NULL,NULL) <= 0)
                { printf("写失败.\n"); }
                else
                { send(eventfd,buffer,strlen(buffer),0); }      	
   	   }
        }
 
    }

  }
  return 0;
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

