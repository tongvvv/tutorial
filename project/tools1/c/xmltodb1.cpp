#include"_public.h"
#include"_mysql.h"

CLogFile logfile;
connection conn; //连接数据库
CPActive PActive;  //进程心跳

struct st_arg
{
  char connstr[101];   //数据库的连接参数
  char charset[51];    //数据库的字符集
  char inifilename[301];//数据入库的参数配置文件
  char xmlpath[301];    //待入库xml文件存放的目录
  char xmlpathbak[301]; //xml文件入库后的备份目录
  char xmlpatherr[301]; //xml文件入库失败后存放的目录
  int  timevl;          //本程序运行的时间间隔，本程序常驻内存
  int  timeout;         //超时时间
  char pname[51];       //进程名
} starg;

void _help();
bool _xmltoarg(char* strxmlbuffer); //解析xml并存入st_arg结构体中
void EXIT(int sig);

int main(int argc, char* argv[])
{
  if(argc!=3)
  {
     _help(); return -1;
  }  
  
  //CloseIOAndSignal();
  signal(2,SIG_IGN); signal(15,SIG_IGN);
 
  if(logfile.Open(argv[1]) == false )
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
  printf("Using: /project/tools1/bin/xmltodb logfilename xmlbuffer.\n\n");
  printf("Example: /project/tools1/bin/procctl 10 /project/tools1/bin/xmltodb /log/idc/xmltodb_vip1.log "\
          "\"<connstr>101.34.83.118,root,admin,mysql,3306</connstr><charset>utf8</charset><inifilename>/project/tools1/ini/xmltodb.xml</inifilename><xmlpath>/idcdata/xmltodb/vip1</xmlpath><xmlpathbak>/idcdata/xmltodb/vip1bak</xmlpathbak><xmlpatherr>/idcdata/xmltodb/vip1err</xmlpatherr><timevl>5</timevl><timeout>50</timeout><pname>xmltodb_vip1</pname>\" \n\n");
  printf("本程序是数据中心的公共模块，用于把xml文件入库到MYSQL表中.\n");
  printf("logfilename   本程序运行的日志文件。\n");
  printf("xmlbuffer     本程序运行的参数，用xml表示，具体如下：\n\n");
  printf("connstr       数据库的连接参数,格式：ip,username,password,dbname,port.\n");
  printf("charset       数据库的字符集，这个参数要与数据库保持一致，否则会出现中文乱码的情况.\n");
  printf("inifilename   数据入库的参数配置文件。\n");
  printf("xmlpath       待入库xml文件存放的目录.\n");
  printf("xmlpathbak    xml入库成功后的备份目录.\n");
  printf("xmlpatherr    xml入库失败后的存放目录.\n");
  printf("timevl        本程序的时间间隔，单位: 秒 ，视业务需求而定，2-30之间.\n");
  printf("timeout       本程序的超时时间，单位：秒 ，视xml文件大小而定，建议设置30以上.\n");
  printf("pname         进程名.\n\n");
}

bool _xmltoarg(char* strxmlbuffer)
{
  memset(&starg,0,sizeof(struct st_arg)); 
  char connstr[101];   //数据库的连接参数
  char charset[51];    //数据库的字符集
  char inifilename[301];//数据入库的参数配置文件
  char xmlpath[301];    //待入库xml文件存放的目录
  char xmlpathbak[301]; //xml文件入库后的备份目录
  char xmlpatherr[301]; //xml文件入库失败后存放的目录
  int  timevl;          //本程序运行的时间间隔，本程序常驻内存
  int  timeout;         //超时时间
  char pname[51];       //进程名

  GetXMLBuffer(strxmlbuffer,"connstr",starg.connstr,100);
  if(strlen(starg.connstr) == 0) { logfile.Write("constr is null\n"); return false;}

  GetXMLBuffer(strxmlbuffer,"charset",starg.charset,50);  //数据库的字符集
  if(strlen(starg.charset) == 0) { logfile.Write("charset is null\n"); return false;}

  GetXMLBuffer(strxmlbuffer,"inifilename",starg.inifilename,300);
  if(strlen(starg.inifilename) == 0) { logfile.Write("inifilename is null\n"); return false;}

  GetXMLBuffer(strxmlbuffer,"xmlpath",starg.xmlpath,300);
  if(strlen(starg.xmlpath) == 0) { logfile.Write("xmlpath is null\n"); return false;}

  GetXMLBuffer(strxmlbuffer,"xmlpathbak",starg.xmlpathbak,300);
  if(strlen(starg.xmlpathbak) == 0) { logfile.Write("xmlpathbak is null\n"); return false;}

  GetXMLBuffer(strxmlbuffer,"xmlpatherr",starg.xmlpatherr,300);
  if(strlen(starg.xmlpatherr) == 0) { logfile.Write("xmlpatherr is null\n"); return false;}

  GetXMLBuffer(strxmlbuffer,"timevl",&starg.timevl);
  if(starg.timevl < 2) {starg.timevl=2;}
  if(starg.timevl > 30) {starg.timevl=30;}

  GetXMLBuffer(strxmlbuffer,"timeout",&starg.timeout);
  if(starg.timeout == 0 ) { logfile.Write("timeout is null\n"); return false;}
  if(starg.timeout < 20) {starg.timeout = 20;}
 
  GetXMLBuffer(strxmlbuffer,"pname",starg.pname,50);
  if(strlen(starg.pname) == 0) { logfile.Write("pname is null\n"); return false;}


  return true; 
}
















