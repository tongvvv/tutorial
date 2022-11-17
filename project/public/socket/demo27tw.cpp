#include "../_public.h"
 
int main(int argc,char *argv[])
{
  if (argc!=3)
  {
    printf("Using:./demo07 ip port\nExample:./demo07 127.0.0.1 5005\n\n"); return -1;
  }

  CTcpClient TcpClient;

  // 向服务端发起连接请求。
  if (TcpClient.ConnectToServer(argv[1],atoi(argv[2]))==false)
  {
    printf("TcpClient.ConnectToServer(%s,%s) failed.\n",argv[1],argv[2]); return -1;
  }

  char buffer[102400] ={};

  sprintf(buffer,\
                 "GET / HTTP/1.1\r\n"\
                 "Host: %s:%s\r\n"\
                 "\r\n",argv[1],argv[2]);

  send(TcpClient.m_connfd,buffer,strlen(buffer),0);
 
  memset(buffer,0,sizeof(buffer));
  recv(TcpClient.m_connfd,buffer,sizeof(buffer),0);
  printf("%s\n",buffer);
}
