#include"_public.h"
#include"_mysql.h"

CLogFile logfile;
connection conn;
CPActive PActive;

void EXIT(int sig);
bool _obtmindtodb(char* pathname, char* connstr, char* charset);//主函数

struct st_zhobtmind
{
  char obtid[11];
  char ddatetime[21];
  char t[11];
  char p[11];
  char u[11];
  char wd[11];
  char wf[11];
  char r[11];
  char vis[11];
} stzhobtmind;

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

  conn.commit();
    

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
  sqlstatement stmt;

  //打开目录 
  if(Dir.OpenDir(pathname,"*.xml") == false)
  {
    logfile.Write("Dir.Open(%s) failed.\n",pathname); return false;   
  }

  CFile File;

  int totalcount=0; //文件的总记录数
  int insertcount=0; //成功插入的记录数
  CTimer Timer;  //计时器 记录每个文件的处理时间
  while(true)
  {
    //读取目录得到文件
    if(Dir.ReadDir()==false) { break; }
 
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
    if(stmt.m_state == 0)
    {
      stmt.connect(&conn);
      stmt.prepare("insert into T_ZHOBTMIND (obtid,ddatetime,t,p,u,wd,wf,r,vis) values (:1,str_to_date(:2,'%%Y%%m%%d%%H%%i%%s'),:3,:4,:5,:6,:7,:8,:9)");
      stmt.bindin(1,stzhobtmind.obtid,10);
      stmt.bindin(2,stzhobtmind.ddatetime,14);
      stmt.bindin(3,stzhobtmind.t,10);
      stmt.bindin(4,stzhobtmind.p,10);
      stmt.bindin(5,stzhobtmind.u,10);
      stmt.bindin(6,stzhobtmind.wd,10);
      stmt.bindin(7,stzhobtmind.wf,10);
      stmt.bindin(8,stzhobtmind.r,10);
      stmt.bindin(9,stzhobtmind.vis,10);
    }

    totalcount=0; insertcount=0;

    //打开文件
    if(File.Open(Dir.m_FullFileName,"r")==false)
    {
      logfile.Write("File.Open(%s) failed.\n",Dir.m_FullFileName); return false; 
    }
    
    char strBuffer[1001]; //存放读取的一行
    while(true)
    {
      //处理文件中的每一行
      if(File.FFGETS(strBuffer,1000,"<endl/>") == false) {break;}
      //logfile.Write("strBuffer=%s\n",strBuffer);
      totalcount++;
      memset(&stzhobtmind,0,sizeof(struct st_zhobtmind));
      GetXMLBuffer(strBuffer,"obtid",stzhobtmind.obtid,10);
      GetXMLBuffer(strBuffer,"ddatetime",stzhobtmind.ddatetime,14);
      char tmp[11];
      GetXMLBuffer(strBuffer,"t",tmp,10);
      if(strlen(tmp)>0) { snprintf(stzhobtmind.t,10,"%d",(int)(atof(tmp)*10));}
      GetXMLBuffer(strBuffer,"p",tmp,10);
      if(strlen(tmp)>0) { snprintf(stzhobtmind.p,10,"%d",(int)(atof(tmp)*10));}
      GetXMLBuffer(strBuffer,"u",stzhobtmind.u,10);
      GetXMLBuffer(strBuffer,"wd",stzhobtmind.wd,10);
      GetXMLBuffer(strBuffer,"wf",tmp,10);
      if(strlen(tmp)>0) { snprintf(stzhobtmind.wf,10,"%d",(int)(atof(tmp)*10));}
      GetXMLBuffer(strBuffer,"r",tmp,10);
      if(strlen(tmp)>0) { snprintf(stzhobtmind.r,10,"%d",(int)(atof(tmp)*10));}
      GetXMLBuffer(strBuffer,"vis",tmp,10);
      if(strlen(tmp)>0) { snprintf(stzhobtmind.vis,10,"%d",(int)(atof(tmp)*10));}
      
      //把结构体中的数据插入表中
      if(stmt.execute() != 0 )
      {
        //插入失败原因主要有两点：数据重复，数据非法
        //如果数据重复，跳过它继续；如果数据非法，记录下非法数据
        if(stmt.m_cda.rc != 1062)
        {
          logfile.Write("非法数据Buffer=%s.\n",strBuffer);
          logfile.Write("stmt.execute() failed.\n%s\n%s\n",stmt.m_sql,stmt.m_cda.message);  
        }
      }
      else 
      { insertcount++; }          
    } //处理单个文件的while(true)结束

    //删除文件，提交事务
    //File.CloseAndRemove();
    conn.commit();
    logfile.Write("已处理文件%s (totalcount=%d,insertcount=%d)，耗时%.2f秒。\n",Dir.m_FullFileName,totalcount,insertcount,Timer.Elapsed());
  }   
}










