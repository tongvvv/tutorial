//这是一个工具程序，执行一个sql脚本文件

#include"_public.h"
#include"_mysql.h"

CLogFile logfile;
connection conn;
CPActive PActive;

void EXIT(int sig);

int main(int argc, char* argv[])
{
  //帮助文档
  if(argc != 5)
  {
    printf("\n");
    printf("Using: ./execsql sqlfile connstr charset logfile.\n\n");
    printf("Example: /project/tools1/bin/procctl 120 /project/tools1/bin/execsql /project/idc1/sql/cleardata.sql \"101.34.83.118,root,admin,mysql,3306\" utf8 /log/idc/execsql.log\n\n ");

    printf("这是一个工具程序，用于执行一个sql脚本。\n");
    printf("sqlfile  sql脚本文件名，每条sql语句可以多行书写，用分号表示结束，不支持注释。\n");
    printf("connstr 数据库连接参数： ip,username,password,dbname,port。\n");
    printf("charset 数据库的字符集。\n");
    printf("logfile 本程序运行的日志文件名。\n");
    printf("程序每200秒运行一次，由procctl调度。\n");
   
    return -1;
  } 
 
  //CloseIOAndSignal(); signal(2,EXIT); signal(15,EXIT);
  
  //打开日志文件
  if(logfile.Open(argv[4],"a+")==false)
  {
    printf("open(%s) failed\n",argv[4]);
    return -1;
  }
 
  PActive.AddPInfo(10,"obtcodetodb"); //心跳时间

  //连接数据库
  if(conn.connecttodb(argv[2],argv[3],1) != 0)
  {
    logfile.Write("connect database(%s) failed.\n%s\n",argv[2],conn.m_cda.message); return -1;
  } 
 
  logfile.Write("connect database(%s) ok.\n",argv[2]);
 
  CFile File;
  
  if(File.Open(argv[1],"r")==false)
  {
    logfile.Write("File.Open(%s) failed.\n",argv[1]);
  }

  char strsql[1001]; //存放从sql文件中读取的SQL语句
 
  while(true)
  {
    memset(strsql,0,sizeof(strsql));  
  
    if(File.FFGETS(strsql,1000,";")==false) {break;} //得到以分号结束的一行

    strsql[strlen(strsql)-1] = '\0';
  
    logfile.Write("%s\n",strsql);
 
    int iret = conn.execute(strsql); //执行sql语句
 
    if(iret == 0) { logfile.Write("execl ok (rpc=%d).\n",conn.m_cda.rpc);} 
    else { logfile.Write("execl failed(%s).\n",conn.m_cda.message); }
 
    PActive.UptATime();
  }
  logfile.WriteEx("\n");
  return 0;
}

void EXIT(int sig)
{
  logfile.Write("收到信号%d,程序退出。\n",sig);
  exit(0);
}



