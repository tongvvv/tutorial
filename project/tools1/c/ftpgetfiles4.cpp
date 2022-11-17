#include"_public.h"
#include"_ftp.h"

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
  char listfilename[301];//下载前列出服务器文件名的文件
  int ptype;               //下载后服务器文件的处理方式：1-什么也不做，2-删除，3-备份
  char remotepathbak[301]; //下载后服务器文件的备份目录。
  char okfilename[301];   //已下载成功文件名清单
} starg;

struct st_fileinfo
{
   char filename[301];
   char mtime[21];
};

vector<struct st_fileinfo> vlistfile1; //已下载成功文件名的容器，从okfilename中加载
vector<struct st_fileinfo> vlistfile2; //存放下载前列出服务器文件名的窗口
vector<struct st_fileinfo> vlistfile3; //本次不需要下载的文件的容器
vector<struct st_fileinfo> vlistfile4; //本次需要下载的文件的容器

void EXIT(int sig);
void _help();
bool _xmltoarg(char* strxmlbuffer); //解析xml并存入st_arg结构体中
bool _ftpgetfiles(); //下载文件的主函数
bool LoadListFile(); //把ftp.nlist()方法获取到的list文件加载到容器vlistfile2中

//加载okfilename文件中内容到vlistfile1中
bool LoadOKFile();
//比较vlistfile1和vlistfile2，得到vlistfile3和vlistfile4
bool CompVector();
//把容器vlistfile3中的内容写入okfilename文件，覆盖之前旧okfilename文件
bool WriteToOKFile();
//如果ptype==1，把下载成功的文件记录追加到okfilename中
bool AppendToOKFile(struct st_fileinfo* stfileinfo);

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

  if(ftp.login(starg.host,starg.username,starg.password,starg.mode) == false)
  {
    logfile.Write("ftp.login(%s %s %s) failed.\n",starg.host,starg.username,starg.password); return -1;
  }

  logfile.Write("ftp.login(%s %s %s) ok.\n",starg.host,starg.username,starg.password);

  _ftpgetfiles();   


  ftp.logout();
  
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
  printf("Example:/project/tools1/bin/procctl 30 /project/tools1/bin/ftpgetfiles /log/idc/ftpgetfiles_surfdata.log \"<host>47.107.41.197:21</host><mode>1</mode><username>tw</username><password>123456</password><localpath>/idcdata/surfdata</localpath><remotepath>/tmp/idc/surfdata</remotepath><matchname>SURF_ZH*.xml,SURF_ZH*.csv</matchname><listfilename>/idcdata/ftplist/ftpgetfiles_surfdata.list</listfilename><ptype>1</ptype><remotepathbak>/tmp/idc/surfdatabak</remotepathbak><okfilename>/idcdata/ftplist/ftpgetfiles_surfdata.xml</okfilename> \" \n\n"); 

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
           "不匹配的文件不会被下载，本字段尽可能设置精确，不建议用*匹配全部的文件\n");
  printf("<listfilename>/idcdata/ftplist/ftpgetfiles_surfdata.list</listfilename> 下载前列出服务器文件名的文件。\n");
  printf("<ptype>1</ptype> 文件下载成功后，远程服务器文件的处理方式：1-什么也不做，2-删除，3-备份，如果为3，还要指定备份的目录.\n");
  printf("<remotepathbak>/tmp/idc/surfdatabak</remotepathbak>文件下载成功后，服务器文件的备份目录，只有当ptyoe==3时才有效。\n");
  printf("<okfilename>/idcdata/ftplist/ftpgetfiles_surfdata.xml</okfilename> 已成功下载文件名清单，只有当ptype==1时才有效\n\n\n");
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
 
  GetXMLBuffer(strxmlbuffer,"listfilename",starg.listfilename,300); //下载前列出服务器文件名的文件
  if(strlen(starg.listfilename) == 0) { logfile.Write("listfilename is null\n"); return false;  }

  GetXMLBuffer(strxmlbuffer,"ptype",&starg.ptype); //下载后服务器文件的处理方式
  if(starg.ptype != 1 && starg.ptype != 2 && starg.ptype != 3) { logfile.Write("ptype is error\n"); return false;  }

  GetXMLBuffer(strxmlbuffer,"remotepathbak",starg.remotepathbak,300); //下载后服务器文件的备份目录
  if( starg.ptype==3 && (strlen(starg.remotepathbak) == 0) ) { logfile.Write("remotepathbak is null\n"); return false;  }

  GetXMLBuffer(strxmlbuffer,"okfilename",starg.okfilename,300); //下载成功文件名清单    
  if( starg.ptype==3 && (strlen(starg.okfilename) == 0) ) { logfile.Write("okfilename is null\n"); return false;  }

  return true; 
}

bool _ftpgetfiles()
{
  if( ftp.chdir(starg.remotepath) == false)
  {
    logfile.Write("ftp.chdir(%s) failed.\n",starg.remotepath); return false;
  }
   
  if(ftp.nlist(".",starg.listfilename) == false) 
  {
    logfile.Write("ftp.nlist(%s) failed",starg.remotepath); return false;
  }     
 
  //把ftp.nlist()方法获取的list文件加载到容器vlistfile2中
  if(LoadListFile()==false) 
  {
    logfile.Write("LoadListFile() failed.\n"); return false;
  }
   
  if(starg.ptype==1)
  {
     //加载okfilename文件中内容到vlistfile1中
     LoadOKFile();
     //比较vlistfile1和vlistfile2，得到vlistfile3和vlistfile4
     CompVector();
     //把容器vlistfile3中的内容写入okfilename文件，覆盖之前旧okfilename文件
     WriteToOKFile();
     //把vlistfile4中的内容复制到vlistfile2中
     vlistfile2.clear(); vlistfile2.swap(vlistfile4);
  } 
  
  //注意，vlistfile2中存放的是需要下载的文件清单，已经过处理。
  char strremotefilename[301],strlocalfilename[301];
  for(int ii=0 ;ii<vlistfile2.size(); ii++)
  {
    SNPRINTF(strremotefilename,sizeof(strremotefilename),300,"%s/%s",starg.remotepath,vlistfile2[ii].filename);
    SNPRINTF(strlocalfilename,sizeof(strlocalfilename),300,"%s/%s",starg.localpath,vlistfile2[ii].filename);
    
    //调用ftp.get()从服务器下载文件
    logfile.Write("get %s ...",strremotefilename);
   
    if(ftp.get(strremotefilename,strlocalfilename) == false) 
    {
      logfile.Write("failed.\n"); return false;
    }
     
    logfile.WriteEx("ok.\n");
    
    //下载文件后服务器文件处理方式
    switch(starg.ptype)
    {
       case 1: //把文件名存入okfilename文件中
             {
	       AppendToOKFile(&vlistfile2[ii]);
               break;
             }
       case 2: //删除 
             {
               if( ftp.ftpdelete(strremotefilename) == false) 
               {
                  logfile.Write("ftp.delete(%s) failed\n",strremotefilename);
                  return false;
               }
               break;
            }

       case 3: //备份 
             {
               char strremotefilenamebak[301];
	       SNPRINTF(strremotefilenamebak,sizeof(strremotefilenamebak),300,"%s/%s",starg.remotepathbak,vlistfile2[ii].filename);
               if(ftp.ftprename(strremotefilename,strremotefilenamebak)== false) 
               {
                  logfile.Write("ftp.ftprename(%s,%s) failed\n",strremotefilename,strremotefilenamebak); 
                  return false;
               }
 	       break;
             }

      default:break;
    }
  }   
  
  return true; 
}

bool LoadListFile()
{
  vlistfile2.clear();

  CFile File;

  if(File.Open(starg.listfilename,"r")==false)
  {
    logfile.Write("(File.Open(%s) failed.\n",starg.listfilename); return false;
  }

  struct st_fileinfo stfileinfo;
  
  while(true)
  {
    memset(&stfileinfo,0,sizeof(struct st_fileinfo));
    
    if(File.Fgets(stfileinfo.filename,300,true) == false ) { break; }

    if(MatchStr(stfileinfo.filename,starg.matchname) == false) {continue;}   

    vlistfile2.push_back(stfileinfo);
  }

  return true;
}

bool LoadOKFile()
{
  vlistfile1.clear();

  CFile File;
  //注意，如果程序是第一次运行，okfilename不存在，并不是错误，所以也返回true。
  if( (File.Open(starg.okfilename,"r")) == false)
  {
    return true;         
  }

  struct st_fileinfo stfileinfo;
  
  while(true)
  { 
    memset(&stfileinfo,0,sizeof(struct st_fileinfo));
    
    if(File.Fgets(stfileinfo.filename,300,true) == false ) { break; }
    
    vlistfile1.push_back(stfileinfo);
  }

  return true;
}

bool CompVector()
{
   vlistfile3.clear(); vlistfile4.clear();
   int ii,jj;
   //遍历容器2
   for(ii=0 ; ii<vlistfile2.size(); ii++)
   {
      for(jj=0 ; jj<vlistfile1.size(); jj++)
      {
        //如果在容器1中找到了容器2中的文件名，说明不需要下载
        if(strcmp(vlistfile2[ii].filename,vlistfile1[jj].filename) == 0) 
        {
            vlistfile3.push_back(vlistfile2[ii]); break;
        } 
      }
      //如果未找到，说明需要下载
      if(jj == vlistfile1.size())
      {
        vlistfile4.push_back(vlistfile2[ii]); 
      }
   }
   return true;
}

bool WriteToOKFile()
{
  CFile File;
  if(File.Open(starg.okfilename,"w") == false)
  {
    logfile.Write("File.Open(%s) failed.\n",starg.okfilename); return false;
  }
  
  for(int ii=0 ; ii<vlistfile3.size(); ii++)
  {
    File.Fprintf("%s\n",vlistfile3[ii].filename);
  }
  return true;
}

//如果ptype==1，把下载成功的文件记录追加到okfilename中
bool AppendToOKFile(struct st_fileinfo* stfileinfo)
{
  CFile File;
  if(File.Open(starg.okfilename,"a") == false)
  {
    logfile.Write("File.Open(%s) failed.\n",starg.okfilename); return false;
  }
  
  File.Fprintf("%s\n",stfileinfo->filename);
  
  return true; 
}








