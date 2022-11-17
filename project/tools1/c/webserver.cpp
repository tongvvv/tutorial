#include "_public.h"
#include "_ooci.h"

CLogFile   logfile;    // 服务程序的运行日志。
CTcpServer TcpServer;  // 创建服务端对象。

void EXIT(int sig);    // 进程的退出函数。

pthread_mutex_t mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  cond =PTHREAD_COND_INITIALIZER;
vector<int> sockqueue;

struct st_pthinfo
{
  pthread_t pthid; //进程号
  time_t    atime; //最近一次活动时间
};

pthread_t checkpthid;            // 监控线程主函数的子线程的id
pthread_t checkpoolid;           // 监控数据空连接池的子线程的id
pthread_spinlock_t vthidlock;    // vthid容器的锁
vector<struct st_pthinfo> vthid; // 存放全部线程id的容器。
void* thmain(void *arg);         // 线程主函数。
void* checkpool(void* arg);      // 检查数据库连接池的线程函数
void* checkthmain(void* arg);    // 监控线程主函数
void thcleanup(void *arg);       // 线程清理函数。
 
//主程序参数的结构体
struct st_arg
{
  char connstr[101]; //数据库连接参数  
  char charset[51];  //数据库的字符集
  int  port;         //web监听的端口
}starg;

void _help(char* argv[]);

bool _xmltoarg(char* strxmlbuffer);

//有超时机制的Read函数
int ReadT(const int socket,char* buffer, const int size,const int itimeout);

//验证URL中的用户名和密码
bool Login(connection* conn, const char* buffer, const int sockfd);

//从GET请求中获取参数。
bool getvalue(const char *buffer,const char *name,char *value,const int len);

//判断用户是否有调用接口的权限
bool CheckPerm(connection* conn, const char* buffer, const int sockfd);

//执行sql语句，把数据返回给客户端  
bool ExecSQL(connection* conn, const char* buffer, const int sockfd);

/*数据库连接池类*////////////////////
class connpool
{
private:
  struct st_conn
  {
    connection conn ;       //数据库连接
    pthread_mutex_t mutex;  //用于数据库连接的互斥锁
    time_t atime;  	    //上次连接的时间,如果未连接则取0
  } *m_conn;   //数据库连接池
 
  int  m_maxconns;      //最大连接数
  int  m_timeout;       //数据库连接超时时间，单位:秒
  char m_connstr[101]; //数据库连接的参数
  char m_charset[101]; //数据库的字符集
public:
  connpool();
 ~connpool();

  //初始化数据库连接池，初始化锁
  bool init( char* connstr,char* charset,const int maxconns, const int timeout);
  //断开数据库连接，销毁锁，释放内存
  void destroy();

  //从数据库连接池中获得一个空闲的连接，返回连接的地址
  //如果连接池已经用完，或连接数据库失败，返回空
  connection* get();
  //归还数据库连接
  bool free(connection* conn);

  //检查数据库连接池，断开空闲的连接,在服务程序中用一个专用的子线程调用此函数
  void checkpool();
};

connpool oraconnpool; //数据库连接池对象

int main(int argc,char *argv[])
{
  if (argc!=3)
  {
     _help(argv); return -1; 
  }

  // 关闭全部的信号和输入输出。
  // 设置信号,在shell状态下可用 "kill + 进程号" 正常终止些进程
  // 但请不要用 "kill -9 +进程号" 强行终止
  for(int ii=0; ii<100 ; ii++)
  {
    signal(ii,SIG_IGN); 
  }

  signal(SIGINT,EXIT); signal(SIGTERM,EXIT);

  if (logfile.Open(argv[1],"a+")==false) { printf("logfile.Open(%s) failed.\n",argv[2]); return -1; }

  //解析xml到参数结构体starg中
  if( _xmltoarg(argv[2]) == false) EXIT(-1);

  // 服务端初始化。
  if (TcpServer.InitServer(starg.port)==false)
  {
    logfile.Write("TcpServer.InitServer(%s) failed.\n",starg.port); return -1;
  }

  //初始化数据库连接池
  if(oraconnpool.init(starg.connstr,starg.charset,10,50) == false)
  {
     logfile.Write("oraconnpool.init() failed.\n"); return -1;
  }

  //创建数据库连接池监控线程
  if (pthread_create(&checkpoolid,NULL,checkpool,NULL)!=0)
  {
     logfile.Write("pthread_create() failed.\n"); return -1;
  }

  pthread_spin_init(&vthidlock,0);

  //启用10个工作线程，线程比cpu核数略多
  for(int ii=0; ii<10 ; ii++)
  {
    struct st_pthinfo stpthinfo;
    if (pthread_create(&stpthinfo.pthid,NULL,thmain,(void *)(long)ii)!=0)
    {
       logfile.Write("pthread_create() failed.\n"); return -1;
    }
    
    stpthinfo.atime=time(0);
    vthid.push_back(stpthinfo);    // 把线程id放入容器。 
  }
  
  //创建子线程监控线程
  if(pthread_create(&checkpthid,NULL,checkthmain,NULL)!=0)
  {
     logfile.Write("pthread_create() failed.\n"); return -1;
  }

  while (true)
  {
    // 等待客户端的连接请求。
    if (TcpServer.Accept()==false)
    {
      logfile.Write("TcpServer.Accept() failed.\n"); EXIT(-1);
    }

    logfile.Write("客户端（%s）已连接。\n",TcpServer.GetIP());

    //把客户端的socket放入队列，并发送条件信号
    pthread_mutex_lock(&mutex);     //加锁
    sockqueue.push_back(TcpServer.m_connfd);          //入队
    pthread_mutex_unlock(&mutex);   //解锁
    pthread_cond_signal(&cond);      //触发条件
  }
}

void* thmain(void *arg)     // 线程主函数。
{
  int pthnum = (int)(long)arg;
  pthread_cleanup_push(thcleanup,arg);       // 把线程清理函数入栈（关闭客户端的socket）。

  int connfd;    // 客户端的socket。

  pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS,NULL);   // 线程取消方式为立即取消。

  pthread_detach(pthread_self());           // 把线程分离出去。

  char strrecvbuf[1024] = {};
  char strsendbuffer[1024]={};

  while (true)
  {
    pthread_mutex_lock(&mutex);  // 给缓存队列加锁。

    // 如果缓存队列为空，等待，用while防止条件变量虚假唤醒。
    while (sockqueue.size()==0)
    {
      struct timeval now;
      gettimeofday(&now,NULL);
      now.tv_sec += 20;  //取二十秒之后的时间
      //这里cond只等待20秒，20之后还没被唤醒的话，自动醒来，更新线程活动时间，然后继续循环
      pthread_cond_timedwait(&cond,&mutex,(struct timespec*)&now);
      vthid[pthnum].atime=time(0); //更新当前线程的活动时间
    }

    //从缓存队列中获得一条信息
    connfd=sockqueue[0];
    sockqueue.erase(sockqueue.begin()); 

    pthread_mutex_unlock(&mutex);  // 给缓存队列解锁。

    //以下是业务处理代码
    logfile.Write("pthid=%ld(num=%d),connfd=%d\n",pthread_self(),pthnum,connfd);

    memset(strrecvbuf,0,sizeof(strrecvbuf));
    //读取客户端的报文，如果超时或者失败，线程退出
    if(ReadT(connfd,strrecvbuf,sizeof(strrecvbuf),3) <=0 ) {close(connfd); continue;}
   
    //如果不是GET请求报文，不处理，线程退出
    if(strncmp(strrecvbuf,"GET",3) != 0) { close(connfd); continue;}

    logfile.Write("处理GET请求:%s\n",strrecvbuf);

    //连接数据库
    connection *conn=oraconnpool.get();
    //如果数据库连接为空，返回内部错误
    if(conn == NULL)
    {
       memset(strsendbuffer,0,sizeof(strsendbuffer));
       sprintf(strsendbuffer,\
            "HTTP/1.1 200 ok\r\n"\
            "Server: webserver\r\n"\
            "Content-Type: text/html;charset=utf-8\r\n\r\n"\
            "<retcode>-1</retcode><message>internal error.</message>");
       Writen(connfd,strsendbuffer,strlen(strsendbuffer));  
       close(connfd); continue;
    }  
 
    //验证URL中的用户名和密码
    if(Login(conn,strrecvbuf,connfd) == false) { oraconnpool.free(conn); close(connfd); continue;}

    //判断用户是否有调用接口的权限
    if(CheckPerm(conn,strrecvbuf,connfd) == false) { oraconnpool.free(conn); close(connfd); continue;}

    //把响应报文的头部发送给客户端
    char strsendbuffer[1024]={};
    sprintf(strsendbuffer,\
            "HTTP/1.1 200 ok\r\n"\
            "Server: webserver\r\n"\
            "Content-Type: text/html;charset=utf-8\r\n\r\n");
    Writen(connfd,strsendbuffer,strlen(strsendbuffer));

    //执行sql语句，把数据返回给客户端  
    if(ExecSQL(conn,strrecvbuf,connfd)==false) { oraconnpool.free(conn); close(connfd); continue;}

    //更新当前线程的活动时间
    vthid[pthnum].atime = time(0);
 
    oraconnpool.free(conn);  close(connfd);
  }

  pthread_cleanup_pop(1);         // 把线程清理函数出栈。
}

// 检查数据库连接池的线程函数
void* checkpool(void* arg)
{
   while(true)
   {
      oraconnpool.checkpool();
      sleep(30);
   }
}

// 监控线程主函数
void* checkthmain(void* arg)
{
  while(true)
  {
    for(int ii=0; ii<vthid.size(); ii++)
    {
       //超时时间最好要大于处理业务的时间
       //如果已超时
       if((time(0)-vthid[ii].atime) > 25)   
       {
	  logfile.Write("thread %d(%lu) timeout(%d).\n",ii,vthid[ii].pthid,time(0)-vthid[ii].atime);
  
          //取消工作线程
          pthread_cancel(vthid[ii].pthid);
         
          //重新创建工作线程
          if(pthread_create(&vthid[ii].pthid,NULL,thmain,(void*)(long)ii) != 0)
          {
             logfile.Write("pthread_create() failed.\n");  EXIT(-1);
          }

          vthid[ii].atime=time(0);          
       }
    }
    sleep(3);
  }
}

// 进程的退出函数。
void EXIT(int sig)  
{
  // 以下代码是为了防止信号处理函数在执行的过程中被信号中断。
  signal(SIGINT,SIG_IGN); signal(SIGTERM,SIG_IGN);

  logfile.Write("进程退出，sig=%d。\n",sig);

  TcpServer.CloseListen();    // 关闭监听的socket。

  //线程退出的时候会把自身id从容器中删除，size大小会改变，所以这里必须加锁
  pthread_spin_lock(&vthidlock);
  // 取消全部的线程。
  for (int ii=0;ii<vthid.size();ii++)
  {
    pthread_cancel(vthid[ii].pthid);
  }
  pthread_spin_unlock(&vthidlock);

  sleep(1);        // 让子线程有足够的时间退出。

  pthread_cancel(checkpthid);  //取消监控线程
  pthread_cancel(checkpoolid); //取消监控数据库连接池的子线程

  pthread_spin_destroy(&vthidlock);
  pthread_mutex_destroy(&mutex);
  pthread_cond_destroy(&cond);

  exit(0);
}

void thcleanup(void *arg)     // 线程清理函数。
{
  pthread_mutex_unlock(&mutex);

  // 把本线程id从存放线程id的容器中删除。
  pthread_spin_lock(&vthidlock);
  for (int ii=0;ii<vthid.size();ii++)
  {
    if (pthread_equal(pthread_self(),vthid[ii].pthid)) { vthid.erase(vthid.begin()+ii); break; }
  }
  pthread_spin_unlock(&vthidlock);

  logfile.Write("线程%d(%lu)退出。\n",(int)(long)arg,pthread_self());
}

void _help(char* argv[])
{
  printf("Using: /project/tools1/bin/webserver logfile xmlbuffer.\n\n");
  printf("Example: /project/tools1/bin/procctl 10 /project/tools1/bin/webserver /log/idc/webserver.log \"<connstr>qxidc/qxidcpwd@tw_oracle</connstr><charset>Simplified Chinese_China.AL32UTF8</charset><port>8080</port>\"\n\n");

  printf("本程序是数据总线的服务端程序，为数据中心提供http协议的数据访问接口.\n");
  printf("logfile     本程序运行的日志文件.\n");
  printf("xmlbuffer   本程序运行的参数,用xml表示，具体如下：\n");

  printf("connstr     数据库的连接参数,username/passwd@tnsname.\n");
  printf("charset     字符集,要与数据源数据库保持一致.\n");
  printf("port        web监听的端口.\n\n");
}

bool _xmltoarg(char* strxmlbuffer)
{
  memset(&starg,0,sizeof(struct st_arg));

  GetXMLBuffer(strxmlbuffer,"connstr",starg.connstr,100); 
  if(strlen(starg.connstr) == 0) { logfile.Write("connstr is null.\n"); return false;} 

  GetXMLBuffer(strxmlbuffer,"charset",starg.charset,50); 
  if(strlen(starg.charset) == 0) { logfile.Write("charset is null.\n"); return false;} 

  GetXMLBuffer(strxmlbuffer,"port",&starg.port); 
  if(starg.port == 0) { logfile.Write("port is null.\n"); return false;} 

  return true;
}

//有超时机制的Read函数
int ReadT(const int socket,char* buffer, const int size,const int itimeout)
{
  if(itimeout>0)
  {
    struct pollfd  fds;
    fds.fd=socket;
    fds.events=POLLIN;
    int iret;
    if( (iret=poll(&fds,1,itimeout*1000)) <= 0 ) return iret;
  }

  return recv(socket,buffer,size,0);
}

//验证URL中的用户名和密码
bool Login(connection* conn, const char* buffer, const int sockfd)
{
   char username[31]={} ,passwd[31]={}; 
  
   getvalue(buffer,"username",username,30); //获取用户名
   getvalue(buffer,"passwd",passwd,30);   //获取密码

   //查询T_USERINFO表，判断用户名和密码是否存在
   sqlstatement stmt;
   stmt.connect(conn);
   stmt.prepare("select count(*) from T_USERINFO where username=:1 and passwd=:2 and rsts=1");
   stmt.bindin(1,username,30);
   stmt.bindin(2,passwd,30);
   int icount=0; 
   stmt.bindout(1,&icount);
   stmt.execute();
   stmt.next();

   if(icount==0)  //认证失败，返回认证失败的响应报文
   {
      char strbuffer[256]={};

      sprintf(strbuffer,\
              "HTTP/1.1 200 ok\r\n"\
              "Server: webserver\r\n"\
              "Content-Type: text/html;charset=uft-8\r\n\r\n"\
              "<retcode>-1</retcode><message>username or passwd is invailed</message>");
      Writen(sockfd,strbuffer,strlen(strbuffer));

      return false;
   }
  
  return true;
}

//判断用户是否有调用接口的权限
bool CheckPerm(connection* conn, const char* buffer, const int sockfd)
{
   char username[31]={}, intername[31]={};

   getvalue(buffer,"username",username,30);  
   getvalue(buffer,"intername",intername,30);  

   sqlstatement stmt;
   stmt.connect(conn);
   stmt.prepare("select count(*) from T_USERANDINTER where username=:1 and intername =:2 and intername in (select intername from T_INTERCFG where rsts=1)");
   stmt.bindin(1,username,30);
   stmt.bindin(2,intername,30);
   int icount=0; 
   stmt.bindout(1,&icount);
   stmt.execute();
   stmt.next();
  
   if(icount==0)  //认证失败，返回认证失败的响应报文
   {  
      char strbuffer[256]={};
      
      sprintf(strbuffer,\
              "HTTP/1.1 200 ok\r\n"\
              "Server: webserver\r\n"\
              "Content-Type: text/html;charset=uft-8\r\n\r\n"\
              "<retcode>-1</retcode><message>permission denied</message>");
      Writen(sockfd,strbuffer,strlen(strbuffer));
      
      return false;
   }
  
  return true;
}

//执行sql语句，把数据返回给客户端  
bool ExecSQL(connection* conn, const char* buffer, const int sockfd)
{
  //解析接口名
  char intername[31]={};
  getvalue(buffer,"intername",intername,30);
 
  //从接口参数配置表T_INTERCFG中加载接口参数
  char selectsql[1001]={},colstr[301]={},bindin[301]={};

  sqlstatement stmt(conn);
  stmt.prepare("select selectsql,colstr,bindin from T_INTERCFG where intername=:1");
  stmt.bindin(1,intername,30);
  stmt.bindout(1,selectsql,1000);
  stmt.bindout(2,colstr,300);
  stmt.bindout(3,bindin,300);
  stmt.execute();
  stmt.next();

  //准备查询数据的SQL语句
  stmt.prepare(selectsql);

  //绑定输入变量，根据接口配置中的参数列表(bindin字段)，从url中解析出相应的值，绑定到SQL语句中
  CCmdStr CmdStr;
  CmdStr.SplitToCmd(bindin,",");

  char invalue[CmdStr.CmdCount()][101];
  memset(invalue,0,sizeof(invalue));

  for(int ii=0; ii<CmdStr.CmdCount() ; ii++)
  {
     getvalue(buffer,CmdStr.m_vCmdStr[ii].c_str(),invalue[ii],100);
     stmt.bindin(ii+1,invalue[ii],100);
  }
  //绑定输出变量，根据接口配置中的colstr字段，bindout结果集
  CmdStr.SplitToCmd(colstr,",");

  char colvalue[CmdStr.CmdCount()][2001];//存放结果集的数组 
  memset(colvalue,0,sizeof(colvalue)); 

  for(int ii=0; ii<CmdStr.CmdCount() ; ii++)
  {
     stmt.bindout(ii+1,colvalue[ii],2000);
  }

  //执行sql
  char strsendbuffer[4001]={};  

  if(stmt.execute() != 0 )
  {
      sprintf(strsendbuffer,"<retcode>%d</retcode><message>%s</message>\n",stmt.m_cda.rc,stmt.m_cda.message);
      Writen(sockfd,strsendbuffer,strlen(strsendbuffer));
      logfile.Write("stmt.execute() failed.\n%s\n%s\n",stmt.m_sql,stmt.m_cda.message); return false;
  }

  sprintf(strsendbuffer,"<retcode>0</retcode><message>ok</message>\n");
  Writen(sockfd,strsendbuffer,strlen(strsendbuffer));

  //向客户端发送xml内容的头部标签<data>
  Writen(sockfd,"<data>\n",strlen("<data>\n"));

  //获取结果集，拼接xml,发送给客户端
  char strtemp[2001];

  while(true)
  {
    memset(strsendbuffer,0,sizeof(strsendbuffer));
    memset(colvalue,0,sizeof(colvalue));

    if(stmt.next() != 0) {break;}
      
    for(int ii=0 ;ii<CmdStr.CmdCount() ; ii++)
    {
       memset(strtemp,0,sizeof(strtemp));
       snprintf(strtemp,2000,"<%s>%s</%s>",CmdStr.m_vCmdStr[ii].c_str(),colvalue[ii],CmdStr.m_vCmdStr[ii].c_str());
       strcat(strsendbuffer,strtemp);
    }
    strcat(strsendbuffer,"<endl/>\n");
    if( Writen(sockfd,strsendbuffer,strlen(strsendbuffer)) == false) //返回给客户端这行数据
    {
        //socket连接不可用，返回false；
        return false;
    }
  }
  
  //向客户端发送xml内容的尾部标签</data>
  Writen(sockfd,"</data>\n",strlen("</data>\n"));
  
  logfile.Write("intername=%s,count=%d\n",intername,stmt.m_cda.rpc);

  //写接口调用日志表T_USERLOG
  
  return true; 
}

//从GET请求中获取参数。
bool getvalue(const char *buffer,const char *name,char *value,const int len)
{
  value[0]=0;

  char *start,*end;
  start=end=0;

  start=strstr((char *)buffer,(char *)name);
  if (start==0) return false;

  end=strstr(start,"&");
  if (end==0) end=strstr(start," ");

  if (end==0) return false;

  int ilen=end-(start+strlen(name)+1);
  if (ilen>len) ilen=len;

  strncpy(value,start+strlen(name)+1,ilen);

  value[ilen]=0;

  return true;
}

/////////数据库连接池类////////////////////////
connpool::connpool()
{
   m_maxconns=0;
   m_timeout=0;
   memset(m_connstr,0,sizeof(m_connstr));
   memset(m_charset,0,sizeof(m_charset));
   m_conn=0;
}
connpool::~connpool()
{
  destroy();
}

//初始化数据库连接池，初始化锁
bool connpool::init(char* connstr,char* charset,const int maxconns, const int timeout)
{
  //检查数据库连接参数是否可用
  connection conn;
  if(conn.connecttodb(connstr,charset) != 0)
  {
     printf("数据库连接失败.\n%s\n",conn.m_cda.message);
     return false;
  }
  conn.disconnect();

  m_maxconns=maxconns;
  m_timeout=timeout;
  strncpy(m_connstr,connstr,100);
  strncpy(m_charset,charset,100);  

  m_conn = new struct st_conn[m_maxconns];

  for(int ii=0; ii<m_maxconns; ii++)
  {
    pthread_mutex_init(&m_conn[ii].mutex,0);
    m_conn[ii].atime=0;
  }

  return true;
}
//断开数据库连接，销毁锁，释放内存
void connpool::destroy()
{
  for(int ii=0; ii<m_maxconns; ii++)
  {
    m_conn[ii].conn.disconnect();  //断开数据库连接
    pthread_mutex_destroy(&m_conn[ii].mutex); //销毁锁
  }  
  delete[] m_conn;
  m_conn=0;

  m_maxconns=0;
  m_timeout=0;
  memset(m_connstr,0,sizeof(m_connstr));
  memset(m_charset,0,sizeof(m_charset));
}

//从数据库连接池中获得一个空闲的连接，返回连接的地址
connection* connpool::get()
{
  int pos=-1; //用于记录第一个未连接数据库的数组地址

  for(int ii=0; ii<m_maxconns; ii++)
  {
    if(pthread_mutex_trylock(&m_conn[ii].mutex)==0) //尝试加锁 
    {
       if(m_conn[ii].atime>0)  //如果是连接好的状态
       {
          //如果之前有加锁的未连接好的位置,那么释放锁
          if(pos != -1) { pthread_mutex_unlock(&m_conn[pos].mutex); }
          m_conn[ii].atime=time(0);
          printf("取到%d.\n",ii);
          return &m_conn[ii].conn;  //返回数据库连接的地址
       }

         //如果是未连接好的状态,且之前还没有发现其他未连接位置,那么先记录下位置，并且这里不解开锁
         if(pos==-1) {pos=ii;}
         else { pthread_mutex_unlock(&m_conn[ii].mutex);} //如果之前就发现了未连接的位置，这里的锁解开
       
    }
  } 

  if(pos==-1)  //连接池已用完
  { 
    printf("连接池已用完.\n"); return NULL;
  }
  
  //数据库连接池没有用完
  if(m_conn[pos].conn.connecttodb(m_connstr,m_charset) !=0)
  {
    logfile.Write("connect database(%s) failed.\n%s\n",m_conn[pos].conn.m_cda.message);
    pthread_mutex_unlock(&m_conn[pos].mutex);  //释放锁
    return NULL;
  }
  
  //把数据库连接的使用时间设置为当前时间
  printf("新连接%d\n",pos);
  m_conn[pos].atime=time(0);
  return &m_conn[pos].conn;
}

//归还数据库连接
bool connpool::free(connection* conn)
{
  for(int ii=0; ii<m_maxconns; ii++)
  {
    if(&m_conn[ii].conn==conn)
    {
       m_conn[ii].atime=time(0);  //把数据库的时间设置为当前时间
       pthread_mutex_unlock(&m_conn[ii].mutex);
       printf("归还%d.\n",ii);
       return true;
    }
  }

  return false;
}

//检查数据库连接池，断开空闲的连接,在服务程序中用一个专用的子线程调用此函数
void connpool::checkpool()
{
   
  for(int ii=0; ii<m_maxconns; ii++)
  {
    if(pthread_mutex_trylock(&m_conn[ii].mutex)==0) //尝试加锁 
    {
       if(m_conn[ii].atime>0)  //如果是连接好的状态
       {
          //判断连接是否超时
          if( (time(0) - m_conn[ii].atime) > m_timeout)
          { 
             m_conn[ii].conn.disconnect(); //断开数据库连接
             m_conn[ii].atime=0;           //重置数据库连接使用时间
             printf("超时断开 %d.\n",ii);
          }
          else
          {
             //如果没有超时，执行一次sql,验证连接是否有效，如果无效，断开它
             //如果网络断开，或者数据库重启，那么就需要从重新连接数据库，在这里只需要断开连接就行了
             //重连的操作交给get()函数
             if( m_conn[ii].conn.execute("select * from dual") != 0)
             {
      		m_conn[ii].conn.disconnect();   //断开连接
                m_conn[ii].atime=0;           //重置数据库连接使用时间
             }
          }
       }
       pthread_mutex_unlock(&m_conn[ii].mutex); //释放锁 
    }
    //加锁失败表示数据库连接正在使用中，不需要检查
  }  
}








