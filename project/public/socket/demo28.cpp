#include "../_public.h"
#include"_ooci.h"
 
// 解析get请求的参数，从T_ZHOBTMIND1中查询数据，返回给客户端
bool SendHtmlFile(const int sockfd,const char *filename);
//解析url
bool getvalue(const char* strget,const char* name,char* value, int len);

int main(int argc,char *argv[])
{
  if (argc!=2)
  {
    printf("Using:./demo26 port\nExample:./demo26 8080\n\n"); return -1;
  }

  CTcpServer TcpServer;

  // 服务端初始化。
  if (TcpServer.InitServer(atoi(argv[1]))==false)
  {
    printf("TcpServer.InitServer(%s) failed.\n",argv[1]); return -1;
  }

  // 等待客户端的连接请求。
  if (TcpServer.Accept()==false)
  {
    printf("TcpServer.Accept() failed.\n"); return -1;
  }

  printf("客户端（%s）已连接。\n",TcpServer.GetIP());

  char strget[102400];
  char strsend[102400];
  memset(strget,0,sizeof(strget));

  // 接收http客户端发送过来的报文。
  recv(TcpServer.m_connfd,strget,1000,0);

  // 先把响应报文头部发送给客户端。
  memset(strsend,0,sizeof(strsend));
  sprintf(strsend,\
         "HTTP/1.1 200 OK\r\n"\
         "Server: demo26\r\n"\
         "Content-Type: text/html;charset=utf-8\r\n"\
         "Content-Length: 108909\r\n\r\n");
  if (Writen(TcpServer.m_connfd,strsend,strlen(strsend))== false) return -1;

  //logfile.Write("%s",buffer);

  // 解析get请求的参数，从T_ZHOBTMIND1中查询数据，返回给客户端
  SendData(TcpServer.m_connfd,strget);
}

// 解析get请求的参数，从T_ZHOBTMIND1中查询数据，返回给客户端
bool SendHtmlFile(const int sockfd,const char *strget)
{
  //解析参数
  //控制权限
  //接口设计
  //查询条件
  char username[31]={},passwd[31]={},intername[30]={},obtid[11]={},begintime[21]={},endtime[21]={};

  getvalue(strget,"username",username,30);  
  getvalue(strget,"passwd",passwd,30);  
  getvalue(strget,"intername",intername,29);  
  getvalue(strget,"obtid",obtid,10);  
  getvalue(strget,"begintime",begintime,20);  
  getvalue(strget,"endtime",endtime,20);  

  connection conn;
  if(conn.connecttodb("scott/tiger@tw_oracle","Simplified Chinese_China.AL32UTF8")!=0) {return false;}

  sqlstatement stmt(&conn);
  stmt.prepare();
  //....
  return true;
}

//解析url
bool getvalue(const char* strget,const char* name,char* value,int len)
{
  char* start=strstr(strget,name); 
  if(start == NULL) {return false;}

  char* end=strstr(strget,"&");
  if(end == NULL) { strstr(strget," "); }   
  if(end == NULL) {return false;}
 
  int ilen=end-(start+strlen(name)+1);
  if(ilen>len) { ilen=len;}

  strncpy(value,start+strlen(name)+1,ilen);

  value[ilen]='\0';

  return true;
}





