#include"idcapp.h"

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
    printf("Example: /project/tools1/bin/procctl 10 /project/idc1/bin/obtmindtodb /idcdata/surfdata  \"101.34.83.118,root,admin,mysql,3306\" utf8   /log/idc/obtmindtodb.log\n\n ");

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

  PActive.AddPInfo(30,"obtmindtodb");  //进程心跳
  //PActive.AddPInfo(5000,"obtmindtodb");  //调试用
  
  _obtmindtodb(argv[1],argv[2],argv[3]);

  conn.commit();
    

  return 0;
}

void EXIT(int sig)
{
  logfile.Write("收到信号%d,程序退出。\n",sig);
  exit(0);
}

//业务主函数
bool _obtmindtodb(char* pathname, char* connstr, char* charset) 
{
  CDir Dir;

  //打开目录 
  if(Dir.OpenDir(pathname,"*.xml,*.csv") == false)
  {
    logfile.Write("Dir.Open(%s) failed.\n",pathname); return false;   
  }

  CFile File;
  CZHOBTMIND ZHOBTMIND(&conn,&logfile);

  int totalcount=0; //文件的总记录数
  int insertcount=0; //成功插入的记录数
  CTimer Timer;  //计时器 记录每个文件的处理时间
  bool bisxml = false; //true-xml, false-csv

  while(true)
  {
    //读取目录得到文件
    if(Dir.ReadDir()==false) { break; }
 
    if(MatchStr(Dir.m_FullFileName,"*.xml") == true) { bisxml=true; }
    else 
    {
       if(MatchStr(Dir.m_FullFileName,"*.csv") == true) { bisxml=false;}
       else  //如果该文件既不是xml格式也不是csv格式，就跳过它
       {
         continue;  
       }
    }

    //本程序每10秒执行一次,连接数据库开销很大，当目录下有数据文件可入库才连接数据库
    //连接数据库
    if(conn.m_state == 0)
    {
      if(conn.connecttodb(connstr,charset) != 0)
      {
        logfile.Write("connect database(%s) failed.\n%s\n",connstr,conn.m_cda.message);  return -1;
      }
 
      logfile.Write("connect database(%s) ok.\n",connstr);  
    }

    totalcount=0; insertcount=0;

    //打开文件
    if(File.Open(Dir.m_FullFileName,"r")==false)
    {
      logfile.Write("File.Open(%s) failed.\n",Dir.m_FullFileName); return false; 
    }
    
    char strBuffer[1001];

    while(true)
    {
      //处理文件中的每一行
      if(bisxml==true)
      {
        if(File.FFGETS(strBuffer,1000,"<endl/>") == false) {break;}
      }
      else
      {
        if(File.Fgets(strBuffer,1000,true) == false) {break;} //第三个参数为true ，扔掉末尾换行
        if(strstr(strBuffer,"站点") != 0) continue; //把文件的第一行扔掉
      }
      totalcount++;
  
      //拆分strBuffer，存入结构体
      ZHOBTMIND.SplitBuffer(strBuffer,bisxml);

      //把分钟观测数据存入数据库
      if(ZHOBTMIND.InsertTable() == true) { insertcount++;}
      
      
    } //处理单个文件的while(true)结束

    //删除文件，提交事务
    File.CloseAndRemove();
    conn.commit();
    logfile.Write("已处理文件%s (totalcount=%d,insertcount=%d)，耗时%.2f秒。\n",Dir.m_FullFileName,totalcount,insertcount,Timer.Elapsed());
  }   
}










