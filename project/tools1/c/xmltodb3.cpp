#include"_public.h"
#include"_mysql.h"

CLogFile logfile;
connection conn; //连接数据库
CPActive PActive;  //进程心跳

//表的列（字段）信息的结构体
struct st_columns
{
  char colname[31];  //列名
  char datatype[31]; //数据类型,number,data,char三大类
  int  collen;       //列的长度，number固定20，date固定19,char由表结构决定
  int  pkseq;        //如果列是主键的字段，存放主键字段的顺序，从1开始
};

//获取表全部的列和主键列信息的类
class CTABCOLS
{
public:
   CTABCOLS();
   
   int m_allcount;  //全部字段的个数
   int m_pkcount;   //主键字段的个数

   vector<struct st_columns> m_vallcols;  //存放全部字段信息的容器
   vector<struct st_columns> m_vpkcols;   //存放主键字段信息的容器

   char m_allcols[3001];  //全部的字段，以字符串存放，中间用半角的逗号分隔
   char m_pkcols[301];    //全部的主键字段，以字符串存放，中间用半角的逗号分隔

   void initdata();  //成员变量初始化

   //获取表的全部字段信息
   bool allcols(connection* conn, char* tablename);

   //获取表的主键字段信息
   bool pkcols(connection* conn, char* tablename);
};

//数据入库参数的结构体
struct st_xmltotable
{
  char filename[101]; //xml文件匹配规则，用逗号分隔
  char tname[31];     //入库表名
  int  uptbz;         //更新标志，1-更新，2-不更新
  char execsql[301];  //处理xml文件之前，执行的SQL语句
}stxmltotable;

vector<struct st_xmltotable> vxmltotable;//数据入库的参数的容器

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
bool _xmltodb();

//把数据入库的配置文件starg.inifilename加载到vxmltotable容器中；
bool loadxmltotable();
//从vxmltotable容器中查找xmlfilename的入库参数，存放在stxmltotable结构体中；
bool findxmltotable(char* xmlfilename);
//处理xml文件,返回值，0成功，其他失败
int _xmltodb(char* fullfilename,char* filename);
//把xml文件移动至指定目录
bool xmltobakerr(char* fullfilename, char*srcpath, char* dstpath);

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

  //解析xml参数
  if(_xmltoarg(argv[2]) == false) { return -1; }  

  //连接数据库
  if(conn.connecttodb(starg.connstr,starg.charset) != 0)
  { 
    logfile.Write("connect database(%s) failed.\n%s\n",starg.connstr,conn.m_cda.message); return -1;
  }
  logfile.Write("connect database(%s) ok.\n",starg.connstr);
 
  //业务处理主函数
  _xmltodb();


  return 0;
}

bool _xmltodb()
{
  int counter=50; //加载入库参数的计数器，初始化为50是为了第一次进入循环就加载参数  
 
  CDir Dir;  
  while(true)
  {
     //把数据入库的配置文件starg.inifilename加载到容器中
     //据入库的配置文件可能会修改，但修改的频率不高，所以每入库30次，就加载一次配置文件
     if( counter++ > 30)
     {
        counter=0;
        if(loadxmltotable() == false) {return false;}
     }

     //打开starg.xmlpath目录,数据抽取子系统将文件用前缀+时间+后缀+序号+.xml方式命名
     //所以打开目录时要将文件排序，这样先生成的文件先处理；
     if(Dir.OpenDir(starg.xmlpath,"*.XML",10000,false,true)==false)  
     {
        logfile.Write("Dir.OpenDir(%s) failed.\n",starg.xmlpath); 
        return false;
     }

     //遍历目录
     while(true)
     {
        //获得一个xml文件
        if(Dir.ReadDir()==false) {break;}
   
        logfile.Write("处理文件%s...",Dir.m_FullFileName);
        //调用子函数处理xml文件
        int iret=_xmltodb(Dir.m_FullFileName,Dir.m_FileName);

        //处理成功，备份文件，写日志
        if(iret == 0) 
        {
           logfile.WriteEx("ok.\n");
           //把xml文件移动到starg.xmlpathbak参数指定的目录中
           if(xmltobakerr(Dir.m_FullFileName,starg.xmlpath,starg.xmlpathbak) == false)
           {
              return false;
           }
        } 
        //处理失败，分情况。
        //1-没有配置入库参数，2-待入库的表不存在
        if(iret == 1 || iret == 2)
        {
           if(iret==1) {logfile.WriteEx("failed.没有配置入库参数\n");}
           if(iret==2) {logfile.WriteEx("failed.待入库的表不存在\n");}
           //把xml文件移动到starg.xmlpatherr目录中，一般不会发生错误，如果发生，程序将退出
           if(xmltobakerr(Dir.m_FullFileName,starg.xmlpath,starg.xmlpatherr) == false)
           {
              return false;
           }
        }
        //4-数据库错误
        if(iret == 4) //数据库错误，函数返回，程序退出
        {
           logfile.WriteEx("failed.数据库错误\n"); return false;
        }
     }    
     break;
     sleep(starg.timevl);
  }  

  return true;
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

//把数据入库的配置文件starg.inifilename加载到vxmltotable容器中；
bool loadxmltotable()
{
  vxmltotable.clear();
  CFile File;
  if(File.Open(starg.inifilename,"r") == false)
  {
     logfile.Write("File.Open(%s) failed.\n",starg.inifilename); return false;
  }
  
  char strBuffer[501];
  
  while(true)
  {
     if(File.FFGETS(strBuffer,500,"<endl/>") == false) { break; }
     memset(&stxmltotable,0,sizeof(struct st_xmltotable)); 

     GetXMLBuffer(strBuffer,"filename",stxmltotable.filename,100);
     GetXMLBuffer(strBuffer,"tname",stxmltotable.tname,30);
     GetXMLBuffer(strBuffer,"uptbz",&stxmltotable.uptbz);
     GetXMLBuffer(strBuffer,"execsql",stxmltotable.execsql,300);
   
     vxmltotable.push_back(stxmltotable);
  } 
  
  logfile.Write("loadxmltotable(%s) ok.\n",starg.inifilename);
  return true;
} 

//从vxmltotable容器中查找xmlfilename的入库参数，存放在stxmltotable结构体中；
bool findxmltotable(char* xmlfilename)
{
  for(int ii=0 ; ii<vxmltotable.size(); ii++)
  {
     if(MatchStr(xmlfilename,vxmltotable[ii].filename) == true) 
     { 
        memcpy(&stxmltotable,&vxmltotable[ii],sizeof(struct st_xmltotable));
        return true; 
     }
  }
  return false; 
}

//处理xml文件,返回值，0成功，其他失败
int _xmltodb(char* fullfilename,char* filename)
{
  if(findxmltotable(filename)==false) {return 1;}
  
  //获取表全部的字段和主键信息
  CTABCOLS TABCOLS;
  //如果获取失败，应该是数据库连接失效
  //在本程序运行中，如果数据库异常，一定会在这里发现
  if(TABCOLS.allcols(&conn,stxmltotable.tname) == false) {return 4;} 
  if(TABCOLS.pkcols(&conn,stxmltotable.tname) == false) {return 4;} 
  
  //如果表字段信息为空，表不存在，返回2
  if(TABCOLS.m_allcount==0) {return 2;}
  

  //拼接生成插入和更新表数据的SQL

  //prepare SQL语句 ，绑定输入变量 

  //在处理xml文件之前，如果stxmltotable.execsql不为空，就执行它

  //打开xml文件

 /* while(true)
  {
    //读取一行

    //解析xml并存放在已绑定的变量中
     
    //执行SQL语句 
  }*/

  return 0;
}

//把xml文件移动至指定目录
bool xmltobakerr(char* fullfilename, char*srcpath, char* dstpath)
{
  char dstfilename[301]={0}; 
  strcpy(dstfilename,fullfilename);
  
  UpdateStr(dstfilename,srcpath,dstpath,false);
    
  if(RENAME(fullfilename,dstfilename) == false)
  {
    logfile.Write("RENAME(%s,%s) failed.\n",fullfilename,dstfilename); return false;
  }

  return true;
}

CTABCOLS::CTABCOLS()
{
  initdata();
}
   
void CTABCOLS::initdata()
{
   m_allcount=0;
   m_pkcount=0;
   m_vallcols.clear();
   m_vpkcols.clear();
   memset(m_allcols,0,sizeof(m_allcols));
   memset(m_pkcols,0,sizeof(m_pkcols));
}

//获取表的全部字段信息
bool CTABCOLS::allcols(connection* conn,char* tablename)
{
   m_allcount=0;
   m_vallcols.clear();
   memset(m_allcols,0,sizeof(m_allcols));  
 
   struct st_columns stcolumns;
 
   sqlstatement stmt(conn);

   stmt.prepare("select lower(column_name),lower(data_type),character_maximum_length from information_schema.COLUMNS where table_name=:1");
   stmt.bindin(1,tablename,30);
   stmt.bindout(1,stcolumns.colname,30);
   stmt.bindout(2,stcolumns.datatype,30);
   stmt.bindout(3,&stcolumns.collen);

   if(stmt.execute() != 0 ) { return false;}
   
   while(true)
   {
     memset(&stcolumns,0,sizeof(struct st_columns));

     if(stmt.next() != 0) { break;}
  
     //列的数据类型，分为number,date,char三大类     
     if(strcmp(stcolumns.datatype,"char") == 0) strcpy(stcolumns.datatype,"char");
     if(strcmp(stcolumns.datatype,"varchar") == 0) strcpy(stcolumns.datatype,"char");
 
   
     if(strcmp(stcolumns.datatype,"datetime") == 0) strcpy(stcolumns.datatype,"date");
     if(strcmp(stcolumns.datatype,"timestamp") == 0) strcpy(stcolumns.datatype,"date");

     if(strcmp(stcolumns.datatype,"tinyint") == 0) strcpy(stcolumns.datatype,"number");
     if(strcmp(stcolumns.datatype,"smallint") == 0) strcpy(stcolumns.datatype,"number");
     if(strcmp(stcolumns.datatype,"mediumint") == 0) strcpy(stcolumns.datatype,"number");
     if(strcmp(stcolumns.datatype,"int") == 0) strcpy(stcolumns.datatype,"number");
     if(strcmp(stcolumns.datatype,"integer") == 0) strcpy(stcolumns.datatype,"number");
     if(strcmp(stcolumns.datatype,"bigint") == 0) strcpy(stcolumns.datatype,"number");
     if(strcmp(stcolumns.datatype,"float") == 0) strcpy(stcolumns.datatype,"number");
     if(strcmp(stcolumns.datatype,"double") == 0) strcpy(stcolumns.datatype,"number");
     if(strcmp(stcolumns.datatype,"numeric") == 0) strcpy(stcolumns.datatype,"number");
     if(strcmp(stcolumns.datatype,"decimal") == 0) strcpy(stcolumns.datatype,"number");

     //如果数据类型不在上面列出来的范围，忽略它
     if( strcmp(stcolumns.datatype,"char")!=0  &&
         strcmp(stcolumns.datatype,"date")!=0  &&
         strcmp(stcolumns.datatype,"number")!=0  ) {continue;}       

     //如果数据类型是date，长度设置为19，yyyy-mm-dd hh:mi:ss
     if(strcmp(stcolumns.datatype,"date") == 0) { stcolumns.collen=19;}
     //如果数据类型是number,长度设置为20
     if(strcmp(stcolumns.datatype,"number") == 0) { stcolumns.collen=20;}

     strcat(m_allcols,stcolumns.colname);
     strcat(m_allcols,",");

     m_vallcols.push_back(stcolumns);

     m_allcount++;     
   }   
 
   //删掉最后一个逗号
   if(m_allcount > 0 )  { m_allcols[strlen(m_allcols)-1] = 0;  }

   return true;
}

//获取表的主键字段信息
bool CTABCOLS::pkcols(connection* conn, char* tablename)
{
  m_pkcount=0;
  m_vpkcols.clear();
  memset(m_pkcols,0,sizeof(m_pkcols));

  struct st_columns stcolumns;

  sqlstatement stmt(conn);
  stmt.prepare("select lower(column_name),seq_in_index from information_schema.STATISTICS where table_name=:1 and index_name='primary' order by seq_in_index");
  stmt.bindin(1,tablename,30);
  stmt.bindout(1,stcolumns.colname,30);
  stmt.bindout(2,&stcolumns.pkseq);

  if(stmt.execute() != 0 ) {return false;}
 
  while(true)
  {
    memset(&stcolumns,0,sizeof(struct st_columns));
  
    if(stmt.next() != 0) {break;}

    strcat(m_pkcols,stcolumns.colname); strcat(m_pkcols,",");
    m_vpkcols.push_back(stcolumns); 

    m_pkcount++; 
  }  
  
  if(m_pkcount>0) { m_pkcols[strlen(m_pkcols)-1] = 0 ;}

  return true;
}












