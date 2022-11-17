#include"_public.h"
#include"_mysql.h"

CLogFile logfile;
connection conn;
CPActive PActive;

struct st_stcode
{
  char provname[31];//省  
  char obtid[11];   //站号
  char cityname[31]; //站名
  char lat[11];       //纬度
  char lon[11];       //经度
  char height[11];    //海拔高度
};

vector<struct st_stcode> vstcode; //存放全国气象站点参数的容器

bool LoadSTcode(const char* inifile);//把全国气象站点参数加载到vstcode容器中
void EXIT(int sig);


int main(int argc, char* argv[])
{
  //帮助文档
  if(argc != 5)
  {
    printf("\n");
    printf("Using: ./obtcodetodb inifile connstr charset logfile.\n\n");
    printf("Example: /project/tools1/bin/procctl 200 /project/idc1/bin/obtcodetodb /project/idc1/ini/stcode.ini \"101.34.83.118,root,admin,mysql,3306\" utf8 /log/idc/obtcodetodb.log\n\n ");

    printf("本程序用于把全国气象站点参数数据保存到数据库中，如果站点不存在则插入，站点已存在则更新。\n");
    printf("inifile 站点参数文件名（全路径）。\n");
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

  //把全国气象站点参数加载到vstcode容器中
  if(LoadSTcode(argv[1])==false){return -1;}   

  logfile.Write("加载站点参数(%s)成功，站点数(%d).\n",argv[1],vstcode.size());

  //连接数据库
  if(conn.connecttodb(argv[2],argv[3]) != 0)
  {
    logfile.Write("connect database(%s) failed.\n%s\n",argv[2],conn.m_cda.message); return -1;
  } 
 
  logfile.Write("connect database(%s) ok.\n",argv[2]);
 
  struct st_stcode stcode;
  //准备插入的SQL语句
  sqlstatement stmtins(&conn);  

  stmtins.prepare("insert into T_ZHOBTCODE(obtid,cityname,provname,lat,lon,height,upttime) values(:1,:2,:3,:4*100,:5*100,:6*10,now())");

  stmtins.bindin(1,stcode.obtid,10);
  stmtins.bindin(2,stcode.cityname,30);
  stmtins.bindin(3,stcode.provname,30);
  stmtins.bindin(4,stcode.lat,10);
  stmtins.bindin(5,stcode.lon,10);
  stmtins.bindin(6,stcode.height,10);

  //准备更新的SQL语句
  sqlstatement stmtupt(&conn);
  stmtupt.prepare("update T_ZHOBTCODE set cityname=:1,provname=:2,lat=:3*100,lon=:4*100,height=:5*10,upttime=now() where obtid=:6"); 
  stmtupt.bindin(1,stcode.cityname,30);
  stmtupt.bindin(2,stcode.provname,30);
  stmtupt.bindin(3,stcode.lat,10);
  stmtupt.bindin(4,stcode.lon,10);
  stmtupt.bindin(5,stcode.height,10); 
  stmtupt.bindin(6,stcode.obtid,10);
 
  int inscount=0,uptcount=0;
  CTimer Timer;
  for(int ii=0; ii<vstcode.size(); ii++)
  {
    memcpy(&stcode,&vstcode[ii],sizeof(struct st_stcode));

    if(stmtins.execute() != 0)
    {
      if(stmtins.m_cda.rc == 1062)
      {
        if(stmtupt.execute()!=0)
        {
          logfile.Write("stmtupt.execute() failed.\n%s\n%s\n",stmtupt.m_sql,stmtupt.m_cda.message); return -1;
        }
        else
        {
           uptcount++;
        }
      }
      else
      {
        logfile.Write("stmtins.execute() failed.\n%s\n%s\n",stmtins.m_sql,stmtins.m_cda.message); return -1;
      }
    }
    else
    {
      inscount++;   
    }
  }  

  logfile.Write("总记录数%d,插入%d条信息,更新%d条信息,总耗时%.2f秒。\n",vstcode.size(),inscount,uptcount,Timer.Elapsed());
 
  conn.commit(); 
  return 0;
}

void EXIT(int sig)
{
  logfile.Write("收到信号%d,程序退出。\n",sig);
  exit(0);
}

//把站点参数文件加载到vstcode容器中
bool LoadSTcode(const char* inifile)
{  
   CFile File;
   if(File.Open(inifile,"r")==false)
   { logfile.Write("Open file (%s) failed.\n",inifile); return false; }
   
   char strBuffer[301];
   
   CCmdStr CmdStr;
   
   struct st_stcode stcode;
   while(true)
   { 
     if(File.Fgets(strBuffer,300,true)==false) {break;}
     
     CmdStr.SplitToCmd(strBuffer,",",true);
     
     if(CmdStr.CmdCount()!=6) { continue; }  //扔掉无效行
     
     CmdStr.GetValue(0,stcode.provname,30); //省
     CmdStr.GetValue(1,stcode.obtid,10);    //站名
     CmdStr.GetValue(2,stcode.cityname,30);  //站号
     CmdStr.GetValue(3,stcode.lat,10);        //纬度
     CmdStr.GetValue(4,stcode.lon,10);        //经度
     CmdStr.GetValue(5,stcode.height,10);     //海拔

     vstcode.push_back(stcode);
   }
  return true;
}






