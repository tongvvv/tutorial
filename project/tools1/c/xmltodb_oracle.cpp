#include"_tools_oracle.h"

CLogFile logfile; //日志
connection conn; //连接数据库
CPActive PActive;  //进程心跳

CTABCOLS TABCOLS; //获取表全部的列和主键列信息


//数据入库参数的结构体
struct st_xmltotable
{
  char filename[101]; //xml文件匹配规则，用逗号分隔
  char tname[31];     //入库表名
  int  uptbz;         //更新标志，1-更新，2-不更新
  char execsql[301];  //处理xml文件之前，执行的SQL语句
}stxmltotable;

vector<struct st_xmltotable> vxmltotable;//数据入库的参数的容器

//程序启动参数
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

//文件的总记录数，插入行数，更新行数
int totalcount,inscount,uptcount;
//处理xml文件的子函数,返回值，0成功，其他失败
int _xmltodb(char* fullfilename,char* filename);
//把xml文件移动至指定目录
bool xmltobakerr(char* fullfilename, char*srcpath, char* dstpath);
//拼接生成插入和更新表数据的SQL
void crtsql();
char strinsertsql[10241]; //插入表的sql语句
char strupdatesql[10241]; //更新表的sql语句
//处理xml文件之前执行sql语句
bool execsql();

//prepare插入和更新的sql语句，绑定输入变量
#define MAXCOLCOUNT 500    //每个表字段的最大数量
sqlstatement  stmtins,stmtupt;
char *strcolvalue[MAXCOLCOUNT] = {0}; //存放从xml文件每一行解析出来的值
void preparesql();


//解析xml并存放在已绑定的变量中
void splitbuffer(char* strBuffer);    

int main(int argc, char* argv[])
{
  if(argc!=3)
  {
     _help(); return -1;
  }  
  
  //CloseIOAndSignal();
  signal(2,EXIT); signal(15,EXIT);
 
  if(logfile.Open(argv[1]) == false )
  {
     printf("打开日志文件失败（%s）。\n",argv[1]); return -1;
  }

  //解析xml参数
  if(_xmltoarg(argv[2]) == false) { return -1; }  
 
  PActive.AddPInfo(starg.timeout,starg.pname);

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
       
        //连接数据库
        if(conn.m_state == 0)
        {
           if(conn.connecttodb(starg.connstr,starg.charset) != 0)
           {        
              logfile.Write("connect database(%s) failed.\n%s\n",starg.connstr,conn.m_cda.message); return -1;
           }
           logfile.Write("connect database(%s) ok.\n",starg.connstr);
        }

        logfile.Write("处理文件%s...",Dir.m_FullFileName);

        //调用子函数处理xml文件
        int iret=_xmltodb(Dir.m_FullFileName,Dir.m_FileName);
        PActive.UptATime();

        //处理成功，备份文件，写日志
        if(iret == 0) 
        {
           logfile.WriteEx("ok.(%s,totalcount=%d,insert=%d,update=%d)\n",stxmltotable.tname,totalcount,inscount,uptcount);
           //把xml文件移动到starg.xmlpathbak参数指定的目录中
           if(xmltobakerr(Dir.m_FullFileName,starg.xmlpath,starg.xmlpathbak) == false)
           {
              return false;
           }
        } 

        //处理失败，分情况。
        //1-没有配置入库参数，2-待入库的表不存在，5-表的字段数太多
        if(iret == 1 || iret == 2)
        {
           if(iret==1) {logfile.WriteEx("failed.没有配置入库参数\n");}
           if(iret==2) {logfile.WriteEx("failed.待入库的表不存在\n");}
           if(iret==5) {logfile.WriteEx("failed.表的字段数太多\n");}

           //把xml文件移动到starg.xmlpatherr目录中，一般不会发生错误，如果发生，程序将退出
           if(xmltobakerr(Dir.m_FullFileName,starg.xmlpath,starg.xmlpatherr) == false)
           {
              return false;
           }
        }

        //3-打开xml文件错误，这种错误一般不会发生，如果发生，程序退出
        if(iret == 3)
        {
          logfile.WriteEx("failed.打开xml文件失败.\n"); return false;   
        }

        //4-数据库错误
        if(iret == 4) //数据库错误，函数返回，程序退出
        {
           logfile.WriteEx("failed.数据库错误\n"); return false;
        }

        //6-数据库错误，在处理xml文件之前，如果执行stxmltotable.execsql失败，函数返回，程序退出
        if(iret == 6)
        {
           logfile.WriteEx("failed.执行execsql失败\n"); return false;
        }
     }    
     //如果刚才扫描到文件，表示不空闲，可能不断有文件生成，不sleep
     if(Dir.m_vFileName.size() == 0)
     {
       sleep(starg.timevl);
     }
     PActive.UptATime();
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
  printf("Using: /project/tools1/bin/xmltodb_oracle logfilename xmlbuffer.\n\n");
  printf("Example: /project/tools1/bin/procctl 10 /project/tools1/bin/xmltodb_oracle /log/idc/xmltodb_oracle_vip2.log "\
          "\"<connstr>qxidc/qxidcpwd@tw_oracle</connstr><charset>Simplified Chinese_China.AL32UTF8</charset><inifilename>/project/tools1/ini/xmltodb.xml</inifilename><xmlpath>/idcdata/xmltodb/vip2</xmlpath><xmlpathbak>/idcdata/xmltodb/vip2bak</xmlpathbak><xmlpatherr>/idcdata/xmltodb/vip2err</xmlpatherr><timevl>5</timevl><timeout>50</timeout><pname>xmltodb_oracle_vip2</pname>\" \n\n");
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
  totalcount=inscount=uptcount=0;
  //如果配置参数找不到，返回1
  if(findxmltotable(filename)==false) {return 1;}
  
  //释放上次处理xml文件时为字段分配的内存
  for(int ii=0; ii<TABCOLS.m_allcount; ii++)
  {
      if(strcolvalue[ii] != 0) { delete []strcolvalue[ii]; strcolvalue[ii]=0; } //注意指针置空
  }
  
  //获取表全部的字段和主键信息
  //如果获取失败，应该是数据库连接失效
  //在本程序运行中，如果数据库异常，一定会在这里发现
  if(TABCOLS.allcols(&conn,stxmltotable.tname) == false) { return 4;} 
  if(TABCOLS.pkcols(&conn,stxmltotable.tname) == false) {  return 4;} 
  
  //如果表字段信息为空，表不存在，返回2
  if(TABCOLS.m_allcount==0) {return 2;}
  //表的字段数不能超过MAXCOLCOUNT
  if(TABCOLS.m_allcount>MAXCOLCOUNT) {return 5;}

  //为每个字段分配内存
  for(int ii=0 ;ii<TABCOLS.m_allcount; ii++)
  { 
     strcolvalue[ii] = new char[TABCOLS.m_vallcols[ii].collen+1];
  }

  //拼接生成插入和更新表数据的SQL
  crtsql();

  //prepare SQL语句 ，绑定输入变量 
  preparesql();

  //在处理xml文件之前，如果stxmltotable.execsql不为空，就执行它
  if(execsql() == false) {return 6;}

  //打开xml文件
  CFile File;
  if(File.Open(fullfilename,"r")==false)
  {
    conn.rollback(); return 3;
  }

  char strBuffer[10241]={0};
  while(true)
  {
    //读取一行
    if(File.FFGETS(strBuffer,10240,"<endl/>") == false)  {break;}

    totalcount++;

    //解析xml并存放在已绑定的变量中
    splitbuffer(strBuffer);    
  
    //执行SQL语句 
    if(stmtins.execute() != 0)
    {
      if(stmtins.m_cda.rc == 1) //记录已存在
      {
          if(stxmltotable.uptbz == 1) 
          {
             if( stmtupt.execute() != 0 )
             {
               //如果update失败，记录出错的行和错误内容，函数不返回，继续处理数据，略过这一行
               logfile.Write("%s\n",strBuffer);
               logfile.Write("stmtupt.execute() failed.\n%s\n%s\n",stmtupt.m_sql,stmtupt.m_cda.message);

               //数据库连接已失效，无法继续，只能返回
               //3113-在操作过程中服务器关闭，3114-在查询过程中丢失与oracle服务器的连接
               if(stmtupt.m_cda.rc==3113 || stmtupt.m_cda.rc==3114 ) {return 4;}
             }
             else {uptcount++;}
          }
      }
      else
      {
         //如果insert失败，记录出错的行和错误内容，函数不返回，继续处理数据，略过这一行
         logfile.Write("%s\n",strBuffer);
         logfile.Write("stmtins.execute() failed.\n%s\n%s\n",stmtins.m_sql,stmtins.m_cda.message);

         //数据库连接已失效，无法继续，只能返回
         //3113-在操作过程中服务器关闭，3114-在查询过程中丢失与oracle服务器的连接
         if(stmtins.m_cda.rc==3113 || stmtins.m_cda.rc==3114 ) {return 4;}
      }
    }
    else {inscount++;}
 
  }//while(true)
  conn.commit();

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
/*
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
*/
//拼接生成插入和更新表数据的SQL
void crtsql()
{
  memset(strinsertsql,0,sizeof(strinsertsql));
  memset(strupdatesql,0,sizeof(strupdatesql));

  //生成插入表的sql  insert into 表名 (%s) values (%s) ;
  char strinserttp1[3001]={0};
  char strinserttp2[3001]={0};    

  int colseq=1;
  for(int ii=0 ;ii<TABCOLS.m_vallcols.size(); ii++)
  {
    //upttime不需要处理
    if( strcmp(TABCOLS.m_vallcols[ii].colname,"upttime") == 0 ) {continue;} 

    //拼接strinserttp1
    strcat(strinserttp1,TABCOLS.m_vallcols[ii].colname); strcat(strinserttp1,",");

    //keyid字段需要特殊处理
    //拼接strinserttp2,区分date与非date字段
    char strtemp[101]={0};

    if(strcmp(TABCOLS.m_vallcols[ii].colname,"keyid") == 0 )
    {
       SNPRINTF(strtemp,sizeof(strtemp),sizeof(strtemp)-1,"SEQ_%s.nextval",stxmltotable.tname);
    }
    else
    {
      if(strcmp(TABCOLS.m_vallcols[ii].datatype,"date") != 0) 
        { SNPRINTF(strtemp,sizeof(strtemp),sizeof(strtemp)-1,":%d",colseq++);}
      else
        { SNPRINTF(strtemp,sizeof(strtemp),sizeof(strtemp)-1,"to_date(:%d,'yyyymmddhh24miss')",colseq++);}
    }

    strcat(strinserttp2,strtemp); strcat(strinserttp2,",");
  }
  strinserttp1[strlen(strinserttp1)-1] = 0 ;
  strinserttp2[strlen(strinserttp2)-1] = 0 ;
  
  SNPRINTF(strinsertsql,sizeof(strinsertsql),sizeof(strinsertsql)-1,"insert into %s(%s) values(%s)",stxmltotable.tname,strinserttp1,strinserttp2);
  //logfile.Write("%s\n",strinsertsql);
  //
  //如果入库参数指定数据不更新，直接返回
  if(stxmltotable.uptbz != 1) {return;}
  
  //生成更新表的sql
  for(int ii=0; ii<TABCOLS.m_vpkcols.size(); ii++)
  {
     for(int jj=0; jj<TABCOLS.m_vallcols.size(); jj++)
     {
        if(strcmp(TABCOLS.m_vpkcols[ii].colname,TABCOLS.m_vallcols[ii].colname) == 0)
        { TABCOLS.m_vallcols[ii].pkseq = TABCOLS.m_vpkcols[ii].pkseq; break; } 
     }
  }
 
  //先拼接update语句开始的部分
  sprintf(strupdatesql,"update %s set ",stxmltotable.tname);
  
  colseq=1;
  //后拼接update语句set后面的部分
  for(int jj=0; jj<TABCOLS.m_vallcols.size(); jj++)
  {
     //不处理keyid
     if(strcmp(TABCOLS.m_vallcols[jj].colname,"keyid")==0) {continue;}

     //处理更新时间upttime=sysdate,考虑数据库兼容性
     if(strcmp(TABCOLS.m_vallcols[jj].colname,"upttime")==0) 
     { strcat(strupdatesql,"upttime=sysdate,"); continue;} 
 
     //不处理主键
     if(TABCOLS.m_vallcols[jj].pkseq != 0 ) {continue;}  
  
     //区分date与非date
     char strtemp[101]={0};
     if(strcmp(TABCOLS.m_vallcols[jj].datatype,"date") != 0)
      { SNPRINTF(strtemp,sizeof(strtemp),sizeof(strtemp)-1,"%s=:%d",TABCOLS.m_vallcols[jj].colname,colseq++);}
     else
      { SNPRINTF(strtemp,sizeof(strtemp),sizeof(strtemp)-1,"%s=to_date(:%d,'yyyymmddhh24miss')",TABCOLS.m_vallcols[jj].colname,colseq++);}

    strcat(strupdatesql,strtemp); strcat(strupdatesql,",");
  }

  strupdatesql[strlen(strupdatesql)-1] = 0;
  
  //拼接update语句where以及之后的部分
  strcat(strupdatesql," where 1=1 ");  //后面的拼接只要 and 条件 and 条件 就ok了
  
  for(int jj=0; jj<TABCOLS.m_vallcols.size(); jj++)
  {
     if(TABCOLS.m_vallcols[jj].pkseq == 0) {continue;}
  
     //主键字段区分date与非date
     char strtemp[101]={0};
     if(strcmp(TABCOLS.m_vallcols[jj].datatype,"date") != 0)
      { SNPRINTF(strtemp,sizeof(strtemp),sizeof(strtemp)-1," and %s=:%d",TABCOLS.m_vallcols[jj].colname,colseq++);}
     else
      { SNPRINTF(strtemp,sizeof(strtemp),sizeof(strtemp)-1," and %s=to_date(:%d,'yyyymmddhh24miss')",TABCOLS.m_vallcols[jj].colname,colseq++);}
   
     strcat(strupdatesql,strtemp);
  }
  //logfile.WriteEx(" strupdatesql=%s\n",strupdatesql);
}

//prepare插入和更新的sql语句，绑定输入变量
void preparesql()
{
   //绑定插入sql的输入变量
   stmtins.connect(&conn); 
   stmtins.prepare(strinsertsql);
   //logfile.Write("\n%s\n",strinsertsql);
   int colseq=1;

   for(int ii=0; ii<TABCOLS.m_vallcols.size(); ii++)
   {
     //upttime和keyid不需要处理
     if( strcmp(TABCOLS.m_vallcols[ii].colname,"upttime") == 0 ||
     strcmp(TABCOLS.m_vallcols[ii].colname,"keyid") == 0) {continue;}
     
     stmtins.bindin(colseq++,strcolvalue[ii],TABCOLS.m_vallcols[ii].collen);
     //logfile.Write("stmtins.bindin(%d,%s,%d)\n",colseq-1,TABCOLS.m_vallcols[ii].colname,TABCOLS.m_vallcols[ii].collen);
   }            
   
   colseq=1;
   //绑定更新sql的输入变量 
   if(stxmltotable.uptbz != 1) {return;}

   stmtupt.connect(&conn);
   stmtupt.prepare(strupdatesql);
   //logfile.Write("\n%s\n",strupdatesql);
   for(int jj=0; jj<TABCOLS.m_vallcols.size(); jj++)
   {
     //upttime和keyid不需要处理
     if( strcmp(TABCOLS.m_vallcols[jj].colname,"upttime") == 0 ||
     strcmp(TABCOLS.m_vallcols[jj].colname,"keyid") == 0) {continue;}
 
     //不处理主键
     if(TABCOLS.m_vallcols[jj].pkseq != 0 ) {continue;}  
     
     stmtupt.bindin(colseq++,strcolvalue[jj],TABCOLS.m_vallcols[jj].collen);    
     //logfile.Write("stmtupt.bindin(%d,%s,%d)\n",colseq-1,TABCOLS.m_vallcols[jj].colname,TABCOLS.m_vallcols[jj].collen);
   }   
   
   for(int jj=0; jj<TABCOLS.m_vallcols.size(); jj++)
   {
     //处理主键
     if(TABCOLS.m_vallcols[jj].pkseq == 0 ) {continue;}  
     
     stmtupt.bindin(colseq++,strcolvalue[jj],TABCOLS.m_vallcols[jj].collen);    
     //logfile.Write("stmtupt.bindin(%d,%s,%d)\n",colseq-1,TABCOLS.m_vallcols[jj].colname,TABCOLS.m_vallcols[jj].collen);
   }   
}

//处理xml文件之前执行sql语句
bool execsql()
{
  if(strlen(stxmltotable.execsql) == 0 ) {return true;}

  sqlstatement stmt;
  stmt.connect(&conn); 
  stmt.prepare(stxmltotable.execsql); 
  if(stmt.execute() != 0 )
  {
     logfile.Write("stmt.execute() failed.\n%s\n%s\n",stmt.m_sql,stmt.m_cda.message); return false;
  }
  //这里不能提交事务
  //这个sql语句必须和处理xml文件在同一个事务中
  return true;
}

//解析xml并存放在已绑定的变量中
void splitbuffer(char* strBuffer)    
{
  //初始化strcolvalue数组
  for(int ii=0 ; ii<TABCOLS.m_allcount; ii++)
  { memset(strcolvalue[ii],0,TABCOLS.m_vallcols[ii].collen+1); }

  char strtemp[31];
  for(int ii=0; ii<TABCOLS.m_vallcols.size(); ii++)
  {
     //如果是日期字段，只提取数值
     //xml文件中日志只要包含yyyymmddhh24miss就ok了，任意分隔符
     if(strcmp(TABCOLS.m_vallcols[ii].datatype,"date")==0)
     {
        GetXMLBuffer(strBuffer,TABCOLS.m_vallcols[ii].colname,strtemp,TABCOLS.m_vallcols[ii].collen);
        PickNumber(strtemp,strcolvalue[ii],false,false);
        continue;
     }

     //如果是数值字段，只提取数字 + - 小数点
     if(strcmp(TABCOLS.m_vallcols[ii].datatype,"number")==0)
     {
        GetXMLBuffer(strBuffer,TABCOLS.m_vallcols[ii].colname,strtemp,TABCOLS.m_vallcols[ii].collen);
        PickNumber(strtemp,strcolvalue[ii],true,true);
        continue;
     }
     //如果是字符串，直接提取  
     GetXMLBuffer(strBuffer,TABCOLS.m_vallcols[ii].colname,strcolvalue[ii],TABCOLS.m_vallcols[ii].collen);
  }   
}








