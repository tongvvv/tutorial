#include"_public.h"
#include"_ftp.h"

void EXIT(int sig);
void _help();
bool _xmltoarg(char* strxmlbuffer);

CLogFile logfile;

Cftp ftp;

struct st_arg
{
  char host[31];       //远程服务器的IP和端口
  int mode;            //传输模式，1-被动模式，2-主动模式，缺省采用被动模式
  char username[31];   //远程服务器ftp的用户名
  char password[31];   //远程服务器ftp的密码
  char remotepath[301];//远程服务器存放文件的目录
  char localpath[301]; //本地文件存放的目录
  char matchname[101];//待下载文件的匹配规则
} starg;

int main(int argc, char* argv[])
{
  if(argc!=3)
  {
     _help(); return -1;
  }  
  
  //CloseIOAndSignal();
  signal(2,SIG_IGN); signal(15,SIG_IGN);
 
  if(logfile.Open(argv[1],"a+") == false )
  {
     printf("打开日志文件失败（%s）。\n",argv[1]); return -1;
  }

  if(_xmltoarg(argv[2]) == false) { return -1; }  

  
  
  return 0;
}



void EXIT(int sig)
{
  printf("程序退出，sig=%d\n",sig);
  exit(0);
}

void _help()
{ 
  putchar('\n');
  printf("Using:/project/tools1/bin/ftpgetfiles logfilename xmlbuffer\n\n");
  printf("Example:/project/tools1/bin/procctl 30 /project/tools1/bin/ftpgetfiles /log/idc/ftpgetfiles_surfdata.log \"<host>47.107.41.197:21</host><mode>1</mode><username>tw</username><password>123456</password><localpath>/idcdata/surfdata</localpath><remotepath>/tmp/idc/surfdata</remotepath><matchname>SURF_ZH*.xml,SURF_ZH*.csv</matchname>\" \n\n"); 

  printf("本程序是通用的功能模块，用于把远程ftp服务器的文件下载到本地目录。\n"); 
  printf("logfilename 是本程序运行的日志文件。\n");
  printf("xmlbuffer为文件下载的参数，如下：\n");
  printf("<host>47.107.41.197:21</host> 远程服务器的IP和端口\n");
  printf("<mode>1</mode> 传输模式，1-被动模式，2-主动模式，缺省采用被动模式\n");
  printf("<username>tw</username> 远程服务器ftp的用户名\n");
  printf("<password>123456</password> 远程服务器ftp的密码\n");
  printf("<localpath>/idcdata/surfdata</localpath> 本地文件存放的目录\n");
  printf("<remotepath>/tmp/idc/surfdata</remotepath> 远程服务器存放文件的目录\n");
  printf("<matchname>SURF_ZH*.xml,>SURF_ZH*.csv</matchname> 待下载文件的匹配规则。"\
           "不匹配的文件不会被下载，本字段尽可能设置精确，不建议用*匹配全部的文件\n\n\n");
}

bool _xmltoarg(char* strxmlbuffer)
{
  memset(&starg,0,sizeof(struct st_arg)); 

  GetXMLBuffer(strxmlbuffer,"host",starg.host,30); //远程服务器的IP和端口
  if(strlen(starg.host) == 0) { logfile.Write("host is null\n"); return false;}

  GetXMLBuffer(strxmlbuffer,"mode",&starg.mode);  //传输模式，1-被动模式，2-主动模式，缺省采用被动模式  
  if(starg.mode != 2) { starg.mode=1; }

  GetXMLBuffer(strxmlbuffer,"username",starg.username,30);//远程服务器ftp的用户名
  if(strlen(starg.username) == 0) { logfile.Write("username is null\n"); return false;}

  GetXMLBuffer(strxmlbuffer,"password",starg.password,30);   //远程服务器ftp的密码
  if(strlen(starg.password) == 0) { logfile.Write("password is null\n"); return false;}  

  GetXMLBuffer(strxmlbuffer,"remotepath",starg.remotepath,300);//远程服务器存放文件的目录  
  if(strlen(starg.remotepath) == 0) { logfile.Write("remotepath is null\n"); return false;  }

  GetXMLBuffer(strxmlbuffer,"localpath",starg.localpath,300);//本地文件存放的目录  
  if(strlen(starg.localpath) == 0) { logfile.Write("localpath is null\n"); return false;  }

  GetXMLBuffer(strxmlbuffer,"matchname",starg.matchname,100);//待下载文件的匹配规则
  if(strlen(starg.matchname) == 0) { logfile.Write("matchname is null\n"); return false;  }
  
  return true; 
}












