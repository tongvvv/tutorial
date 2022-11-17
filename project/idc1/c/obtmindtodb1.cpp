#include"_public.h"
#include"_mysql.h"

CLogFile logfile;
connection conn;
CPActive PActive;

void EXIT(int sig);
bool _obtmindtodb(char* pathname, char* connstr, char* charset);//主函数

int main(int argc, char* argv[])
{
  //帮助文档
  if(argc != 5)
  {
    printf("\n");
    printf("Using: ./obtmindtodb pathname connstr charset logfile.\n\n");
    printf("Example: /project/tools1/bin/procctl 10 /project/idc1/bin/obtmindtodb /idcdata/surfdata  \"101.34.83.118,root,admin,mysql,3306\" utf8 /log/idc/obtmindtodb.log\n\n ");

    printf("本程序用于把全国气象站点分钟观测数据保存到数据库的T_ZHOBTMIND表中，数据只插入，不更新\n");
    printf("pathname 全国气象站点分钟观测数据存放的目录。\n");
    printf("connstr 数据库连接参数： ip,username,password,dbname,port。\n");
    printf("charset 数据库的字符集。\n");
    printf("logfile 本程序运行的日志文件名。\n");
    printf("程序每10秒运行一次，由procctl调度。\n");
   
    return -1;
  } 
 
  //CloseIOAndSignal(); signal(2,EXIT); signal(15,EXIT);
  
  //打开日志文件
  if(logfile.Open(argv[4],"a+")==false)
  {
    printf("open(%s) failed\n",argv[4]);
    return -1;
  }

  //PActive.AddPInfo(30,"obtmindtodb");  //进程心跳
  PActive.AddPInfo(5000,"obtmindtodb");  //调试用
  
  _obtmindtodb(argv[1],argv[2],argv[3]);
  /*
  //连接数据库
  if(conn.connecttodb(argv[2],argv[3]) != 0)
  {
    logfile.Write("connect database(%s) failed.\n%s\n",argv[2],conn.m_cda.message);  return -1;
  }
 
  logfile.Write("connect database(%s) ok.\n",argv[2]);  


  conn.commit();
  */  

  return 0;
}

void EXIT(int sig)
{
  logfile.Write("收到信号%d,程序退出。\n",sig);
  exit(0);
}

bool _obtmindtodb(char* pathname, char* connstr, char* charset) 
{
  CDir Dir;

  //打开目录 
  if(Dir.OpenDir(pathname,"*xml") == false)
  {
    logfile.Write("Dir.Open(%s) failed.\n",pathname); return false;   
  }

  while(true)
  {
    //读取目录得到文件
    if(Dir.ReadDir()==false) { break; }
    logfile.Write("filename=%s\n",Dir.m_FullFileName);
 
    //打开文件

    /*
    while(true)
    {
       //处理文件中的每一行
       
    }
    */
    //删除文件，提交事务
    
  }   
}












