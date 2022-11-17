#include "_tools.h"

struct st_arg
{
  char localconnstr[101];  // 本地数据库的连接参数。
  char charset[51];        // 数据库的字符集。
  char localtname[31];     // 本地表名。
  char remotecols[1001];   // 远程表的字段列表。
  char localcols[1001];    // 本地表的字段列表。
  char where[1001];        // 同步数据的条件。
  char remoteconnstr[101]; // 远程数据库的连接参数。
  char remotetname[31];    // 远程表名。
  char remotekeycol[31];   // 远程表的键值字段名。
  char localkeycol[31];    // 本地表的键值字段名。
  int  timevl;             // 同步的时间间隔，单位：秒，取值1-30
  int  timeout;            // 本程序运行时的超时时间。
  char pname[51];          // 本程序运行时的程序名。
} starg;

// 显示程序的帮助
void _help(char *argv[]);

// 把xml解析到参数starg结构中
bool _xmltoarg(char *strxmlbuffer);

CLogFile logfile;
CTABCOLS TABCOLS;

connection connloc;   // 本地数据库连接。
connection connrem;   // 远程数据库连接。

// 业务处理主函数。
bool _syncincrementex(bool* bcontinue);

//从本地表starg.localtname获取自增字段最大值，存放在maxkeyvalue全局变量中
long maxkeyvalue;
bool findmaxkey();
 
void EXIT(int sig);

CPActive PActive;

int main(int argc,char *argv[])
{
  if (argc!=3) { _help(argv); return -1; }

  // 关闭全部的信号和输入输出，处理程序退出的信号。
  CloseIOAndSignal();
  signal(SIGINT,EXIT); signal(SIGTERM,EXIT);

  if (logfile.Open(argv[1],"a+")==false)
  {
    printf("打开日志文件失败（%s）。\n",argv[1]); return -1;
  }

  // 把xml解析到参数starg结构中
  if (_xmltoarg(argv[2])==false) return -1;

   PActive.AddPInfo(starg.timeout,starg.pname);
  // 注意，在调试程序的时候，可以启用类似以下的代码，防止超时。
  // PActive.AddPInfo(starg.timeout*100,starg.pname);

  if (connloc.connecttodb(starg.localconnstr,starg.charset) != 0)
  {
    logfile.Write("connect database(%s) failed.\n%s\n",starg.localconnstr,connloc.m_cda.message); EXIT(-1);
  }

  // logfile.Write("connect database(%s) ok.\n",starg.localconnstr);
  //
  //连接数据库
  if(connrem.connecttodb(starg.remoteconnstr,starg.charset) != 0)
  {
    logfile.Write("connect database(%s) failed.\n%s\n",starg.remoteconnstr,connrem.m_cda.message); return false; 
  }  

  if(TABCOLS.allcols(&connloc,starg.localtname) == false)
  {
    logfile.Write("表%s不存在.\n",starg.localtname); return -1;
  }
 
  if(strlen(starg.remotecols)==0) { strcpy(starg.remotecols,TABCOLS.m_allcols);}
  if(strlen(starg.localcols)==0) { strcpy(starg.localcols,TABCOLS.m_allcols);}

  bool bcontinue;
  // 业务处理主函数。
  while(true)
  {
    if(_syncincrementex(&bcontinue)==false) return -1;

    if(bcontinue == false) { sleep(starg.timevl);}
    
    PActive.UptATime();
  }

  return 0;
}

// 显示程序的帮助
void _help(char *argv[])
{
  printf("Using:/project/tools1/bin/syncincrementex logfilename xmlbuffer\n\n");

  printf("       /project/tools1/bin/procctl 10 /project/tools1/bin/syncincrementex /log/idc/syncincrementex_ZHOBTMIND2.log \"<localconnstr>101.34.83.118,root,admin,mysql,3306</localconnstr><charset>utf8</charset><localtname>T_ZHOBTMIND2</localtname><remotecols>obtid,ddatetime,t,p,u,wd,wf,r,vis,upttime,keyid</remotecols><localcols>stid,ddatetime,t,p,u,wd,wf,r,vis,upttime,recid</localcols><remoteconnstr>101.34.83.118,root,admin,mysql,3306</remoteconnstr><remotetname>T_ZHOBTMIND1</remotetname><remotekeycol>keyid</remotekeycol><localkeycol>recid</localkeycol><timevl>2</timevl><timeout>50</timeout><pname>syncincrementex_ZHOBTMIND2</pname>\"\n\n");

  printf("       /project/tools1/bin/procctl 10 /project/tools1/bin/syncincrementex /log/idc/syncincrementex_ZHOBTMIND3.log \"<localconnstr>101.34.83.118,root,admin,mysql,3306</localconnstr><charset>utf8</charset><localtname>T_ZHOBTMIND3</localtname><remotecols>obtid,ddatetime,t,p,u,wd,wf,r,vis,upttime,keyid</remotecols><localcols>stid,ddatetime,t,p,u,wd,wf,r,vis,upttime,recid</localcols><where> and obtid like '54%%' </where><remoteconnstr>101.34.83.118,root,admin,mysql,3306</remoteconnstr><remotetname>T_ZHOBTMIND1</remotetname><remotekeycol>keyid</remotekeycol><localkeycol>recid</localkeycol><timevl>2</timevl><timeout>50</timeout><pname>syncincrementex_ZHOBTMIND3</pname>\"\n\n");

  printf("本程序是数据中心的公共功能模块，采用增量的方法同步MySQL数据库之间的表。\n");

  printf("logfilename   本程序运行的日志文件。\n");
  printf("xmlbuffer     本程序运行的参数，用xml表示，具体如下：\n\n");

  printf("localconnstr  本地数据库的连接参数，格式：ip,username,password,dbname,port。\n");
  printf("charset       数据库的字符集，这个参数要与远程数据库保持一致，否则会出现中文乱码的情况。\n");

  printf("localtname    本地表名。\n");

  printf("remotecols    远程表的字段列表，用于填充在select和from之间，所以，remotecols可以是真实的字段，也可以是函数的返回值或者运算结果。如果本参数为空，就用localtname表的字段列表填充。\n");
  printf("localcols     本地表的字段列表，与remotecols不同，它必须是真实存在的字段。如果本参数为空，就用localtname表的字段列表填充。\n");

  printf("where         同步数据的条件,填充在select remotekeycol from remotetname where remotekeycol>:1之后.\n");

  printf("remoteconnstr 远程数据库的连接参数，格式与localconnstr相同。\n");
  printf("remotetname   远程表名。\n");
  printf("remotekeycol  远程表的键值字段名，必须是唯一的。\n");
  printf("localkeycol   本地表的键值字段名，必须是唯一的。\n");

  printf("timevl        执行同步的时间间隔，单位：秒，取值1-30.\n");
  printf("timeout       本程序的超时时间，单位：秒，视数据量的大小而定，建议设置30以上。\n");
  printf("pname         本程序运行时的进程名，尽可能采用易懂的、与其它进程不同的名称，方便故障排查。\n\n");
}

// 把xml解析到参数starg结构中
bool _xmltoarg(char *strxmlbuffer)
{
  memset(&starg,0,sizeof(struct st_arg));

  // 本地数据库的连接参数，格式：ip,username,password,dbname,port。
  GetXMLBuffer(strxmlbuffer,"localconnstr",starg.localconnstr,100);
  if (strlen(starg.localconnstr)==0) { logfile.Write("localconnstr is null.\n"); return false; }

  // 数据库的字符集，这个参数要与远程数据库保持一致，否则会出现中文乱码的情况。
  GetXMLBuffer(strxmlbuffer,"charset",starg.charset,50);
  if (strlen(starg.charset)==0) { logfile.Write("charset is null.\n"); return false; }

  // 本地表名。
  GetXMLBuffer(strxmlbuffer,"localtname",starg.localtname,30);
  if (strlen(starg.localtname)==0) { logfile.Write("localtname is null.\n"); return false; }

  // 远程表的字段列表，用于填充在select和from之间，所以，remotecols可以是真实的字段，也可以是函数
  // 的返回值或者运算结果。如果本参数为空，就用localtname表的字段列表填充。\n");
  GetXMLBuffer(strxmlbuffer,"remotecols",starg.remotecols,1000);

  // 本地表的字段列表，与remotecols不同，它必须是真实存在的字段。如果本参数为空，就用localtname表的字段列表填充。
  GetXMLBuffer(strxmlbuffer,"localcols",starg.localcols,1000);

  // 同步数据的条件，即select语句的where部分。
  GetXMLBuffer(strxmlbuffer,"where",starg.where,1000);

  // 远程数据库的连接参数，格式与localconnstr相同
  GetXMLBuffer(strxmlbuffer,"remoteconnstr",starg.remoteconnstr,100);
  if (strlen(starg.remoteconnstr)==0) { logfile.Write("remoteconnstr is null.\n"); return false; }

  // 远程表名
  GetXMLBuffer(strxmlbuffer,"remotetname",starg.remotetname,30);
  if (strlen(starg.remotetname)==0) { logfile.Write("remotetname is null.\n"); return false; }

  // 远程表的键值字段名，必须是唯一的。
  GetXMLBuffer(strxmlbuffer,"remotekeycol",starg.remotekeycol,30);
  if (strlen(starg.remotekeycol)==0) { logfile.Write("remotekeycol is null.\n"); return false; }

  // 本地表的键值字段名，必须是唯一的。
  GetXMLBuffer(strxmlbuffer,"localkeycol",starg.localkeycol,30);
  if (strlen(starg.localkeycol)==0) { logfile.Write("localkeycol is null.\n"); return false; }

  //执行同步的时间间隔，单位：秒，取值1-30
  GetXMLBuffer(strxmlbuffer,"timevl",&starg.timevl);
  if(starg.timevl <= 0) {logfile.Write("timevl is null\n"); return false;}
  if(starg.timevl > 30) { starg.timevl=30; }

  // 本程序的超时时间，单位：秒，视数据量的大小而定，建议设置30以上。
  GetXMLBuffer(strxmlbuffer,"timeout",&starg.timeout);
  if (starg.timeout==0) { logfile.Write("timeout is null.\n"); return false; }

  if(starg.timeout < starg.timevl+10) { starg.timeout = starg.timevl+10; }

  // 本程序运行时的进程名，尽可能采用易懂的、与其它进程不同的名称，方便故障排查。
  GetXMLBuffer(strxmlbuffer,"pname",starg.pname,50);
  if (strlen(starg.pname)==0) { logfile.Write("pname is null.\n"); return false; }

  return true;
}

void EXIT(int sig)
{
  logfile.Write("程序退出，sig=%d\n\n",sig);

  connloc.disconnect();

  connrem.disconnect();

  exit(0);
}

/*
create table LK_ZHOBTCODE1
(
   obtid                varchar(10) not null comment '站点代码',
   cityname             varchar(30) not null comment '城市名称',
   provname             varchar(30) not null comment '省名称',
   lat                  int not null comment '纬度，单位：0.01度。',
   lon                  int not null comment '经度，单位：0.01度。',
   height               int not null comment '海拔高度，单位：0.1米。',
   upttime              timestamp not null comment '更新时间。',
   keyid                int not null auto_increment comment '记录编号，自动增长列。',
   primary key (obtid),
   unique key ZHOBTCODE1_KEYID (keyid)
)ENGINE=FEDERATED CONNECTION='mysql://root:mysqlpwd@192.168.174.132:3306/mysql/T_ZHOBTCODE1';

create table LK_ZHOBTMIND1
(
   obtid                varchar(10) not null comment '站点代码。',
   ddatetime            datetime not null comment '数据时间，精确到分钟。',
   t                    int comment '湿度，单位：0.1摄氏度。',
   p                    int comment '气压，单位：0.1百帕。',
   u                    int comment '相对湿度，0-100之间的值。',
   wd                   int comment '风向，0-360之间的值。',
   wf                   int comment '风速：单位0.1m/s。',
   r                    int comment '降雨量：0.1mm。',
   vis                  int comment '能见度：0.1米。',
   upttime              timestamp not null comment '更新时间。',
   keyid                bigint not null auto_increment comment '记录编号，自动增长列。',
   primary key (obtid, ddatetime),
   unique key ZHOBTMIND1_KEYID (keyid)
)ENGINE=FEDERATED CONNECTION='mysql://root:mysqlpwd@192.168.174.132:3306/mysql/T_ZHOBTMIND1';
*/

// 业务处理主函数。
bool _syncincrementex(bool* bcontinue)
{
  CTimer Timer;

  *bcontinue = false;

  //从本地表中获取自增字段最大值，保存在maxkeyvalue中；
  if(findmaxkey() == false) { return false;}

  //拆分starg.localcols,得到本地表字段的个数
  CCmdStr CmdStr;
  CmdStr.SplitToCmd(starg.localcols,",");
  int colcount = CmdStr.CmdCount();  

  char colvalues[colcount][TABCOLS.m_maxcollen+1]; //这个数组存放查询到的记录
  //从远程表查找自增字段大于maxkeyvalue的记录，存放在colvalues数组中
  sqlstatement stmtsel(&connrem);
  stmtsel.prepare("select %s from %s where %s>:1 %s order by %s",starg.remotecols,starg.remotetname,starg.remotekeycol,starg.where,starg.remotekeycol);
  stmtsel.bindin(1,&maxkeyvalue);
  for(int ii=0; ii<colcount ;ii++)
  {
     stmtsel.bindout(ii+1,colvalues[ii],TABCOLS.m_maxcollen+1);
  }
  
  //拼接插入sql语句中values(%s)的内容
  char bindstr[2001] = {0};
  char strtemp[11]; 
 
  for(int ii=0; ii<colcount; ii++)
  {
     memset(strtemp,0,sizeof(strtemp));
     sprintf(strtemp,":%lu,",ii+1);
     strcat(bindstr,strtemp);
  }

  bindstr[strlen(bindstr)-1] = 0;

  //准备插入本地表数据的sql语句
  sqlstatement  stmtins(&connloc); //执行向本地表中插入数据的sql语句
  stmtins.prepare("insert into %s(%s) values(%s) ",starg.localtname,starg.localcols,bindstr);
  for(int ii=0; ii<colcount; ii++)
  {
     stmtins.bindin(ii+1,colvalues[ii],TABCOLS.m_maxcollen+1);  
  }

  if(stmtsel.execute() != 0 )
  {
     logfile.Write("stmtsel.execute() failed.\n%s\n%s\n",stmtsel.m_sql,stmtsel.m_cda.message); return false;
  }
  

  while(true)
  {
    memset(colvalues,0,sizeof(colvalues));

    if(stmtsel.next() != 0 ) {break;}

     if(stmtins.execute() != 0)
     {
        //如果执行出错说明数据库出了问题或者参数配置不对，流程不必继续
        logfile.Write("stmtins.execute() failed.\n%s\n%s\n",stmtins.m_sql,stmtins.m_cda.message);
        return false;
     }
      
     //每一千条记录提交一次
     if(stmtsel.m_cda.rpc%1000 == 0 )
     {
       connloc.commit();
       PActive.UptATime();
     }

  }
 
  if(stmtsel.m_cda.rpc>0) 
  {
     connloc.commit();
     logfile.Write("sync %s to %s(%d rows) in %.2fsec.\n",starg.remotetname,starg.localtname,stmtsel.m_cda.rpc,Timer.Elapsed());
     *bcontinue=true;
  }
 
  return true;
}

bool findmaxkey()
{
  maxkeyvalue=0;
 
  sqlstatement stmt(&connloc);
  stmt.prepare("select max(%s) from %s",starg.localkeycol,starg.localtname);
  stmt.bindout(1,&maxkeyvalue);

  if(stmt.execute() != 0 )
  {
     logfile.Write("stmt.execute() failed.\n%s\n%s\n",stmt.m_sql,stmt.m_cda.message); return false;
  }
  stmt.next();

  logfile.Write("maxkeyvalue=%ld.\n",maxkeyvalue);
 
  return true;
}






