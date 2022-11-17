/*
    程序名：crtsurfdata4.cpp  本程序用于生成全国气象站点观测的分钟数据
    作者：童伟  --仅作为练习
*/
#include"_public.h"

//站点参数结构体
struct st_stcode
{
  char provname[31];//省  
  char obtid[11];   //站号
  char obtname[31]; //站名
  double lat;       //纬度
  double lon;       //经度
  double height;    //海拔高度
};


//全国气象站点分钟观测数据结构体
struct st_surfdata
{
  char obtid[11];           //站点代码
  char ddatetime[21];       //数据时间：格式yyyymmddhh24miss
  int t;                    //气温：单位0.1摄氏度
  int p;		    //气压：单位0.1百帕
  int u;		    //相对湿度：0-100
  int wd;		    //风向：0-360
  int wf; 		    //风速：单位0.1m/s
  int r;		    //降雨量：0.1mm
  int vis;		    //能见度：0.1米
};

CLogFile logfile;  //日志类
char strddatetime[21];//当前时间

vector<struct st_stcode> vstcode; //存放全国气象站点参数的容器
vector<struct st_surfdata> vsurfdata;  //存放全国气象站点分钟观测数据的容器

bool LoadSTcode(const char* inifile);
void CrtSurfData();//模拟生成全国气象站点分钟观测数据并存放在vsurfdata容器中
bool CrtSurfFile(const char* outpath, const char* datafmt);//把容器vsurfdata中的全国气象站点分钟观测数据写入文件。


int main(int argc, char* argv[])
{
  if(argc != 5) 
  {
     printf("Using: ./crtsurfdata4 infile outpath logfile datafmt \n");
     printf("Example: /project/idc1/bin/crtsurfdata4 /project/idc1/ini/stcode.ini /tmp/surfdata /log/idc/crtsurfdata4.log xml,json,csv\n\n");
     printf("infile 全国气象站点参数文件名\n");
     printf("outpath 全国气象站点数据文件存放的目录\n");
     printf("logfile 程序运行的日志文件名\n");
     printf("datafmt 生成数据文件的格式，支持json,xml,csv三种格式，中间用逗号分隔 。\n\n");
     return -1;
  } 

  if(logfile.Open(argv[3]) == false)
  {
     printf("logfile Open(%s) failed.\n",argv[3]); return -1;
  }
 
  logfile.Write("crtsutfdata4 开始运行。\n");

  //这里插入业务代码  
  //把站点参数文件加载到vstcode容器中
  if(LoadSTcode(argv[1])==false){return -1;}
  
  //模拟生成全国气象站点分钟观测数据并存放在vsurfdata容器中
  CrtSurfData();

  //把容器vsurfdata中的全国气象站点分钟观测数据写入文件。
  if(strstr(argv[4],"xml") != 0) { CrtSurfFile(argv[2],"xml"); }
  if(strstr(argv[4],"json") != 0) { CrtSurfFile(argv[2],"json"); }
  if(strstr(argv[4],"csv") != 0) { CrtSurfFile(argv[2],"csv"); }

  logfile.Write("crtsutfdata4 结束运行。\n");

  return 0;
}


//把站点参数文件加载到vstcode容器中
bool LoadSTcode(const char* inifile)
{
   //打开站点参数文件
   CFile File;
   if(File.Open(inifile,"r")==false)
   { logfile.Write("Open file (%s) failed.\n",inifile); return false; }
 
   char strBuffer[301];  

   CCmdStr CmdStr;

   struct st_stcode stcode;
   while(true)
   {
     //从站点参数文件中读取一行数据，如果读取完，退出循环
     if(File.Fgets(strBuffer,300,true)==false) {break;}
     
     logfile.Write("=%s=\n",strBuffer);

     //把数据拆分
     CmdStr.SplitToCmd(strBuffer,",",true);   
  
     if(CmdStr.CmdCount()!=6) { continue; }  //扔掉无效行

     //把站点参数的每个数据项保存到站点参数结构体中     
     CmdStr.GetValue(0,stcode.provname,30); //省
     CmdStr.GetValue(1,stcode.obtid,10);    //站名
     CmdStr.GetValue(2,stcode.obtname,30);  //站号
     CmdStr.GetValue(3,&stcode.lat);        //纬度
     CmdStr.GetValue(4,&stcode.lon);        //经度
     CmdStr.GetValue(5,&stcode.height);     //海拔
     
     //将结构体载入结构体容器中 
     vstcode.push_back(stcode);
   }
    //析构函数自动关闭文件
  return true;
}

//模拟生成全国气象沾点分钟观测数据并存放在vsurfdata容器中
void CrtSurfData()
{
  //播随机数种子
  srand(time(0));  
     
  //获取当前时间，当作观测时间
  memset(strddatetime,0,sizeof(strddatetime));
  LocalTime(strddatetime,"yyyymmddhh24miss");
  
  struct st_surfdata stsurfdata;
  //遍历气象站点参数的vstcode容器
  for(int ii=0 ;ii<vstcode.size(); ii++)
  { 
    memset(&stsurfdata,0,sizeof(stsurfdata));
    
    //用随机数填充分钟观测数据的结构体
    strncpy(stsurfdata.obtid,vstcode[ii].obtid,10);
    strncpy(stsurfdata.ddatetime,strddatetime,14);
    stsurfdata.t= rand()%351;
    stsurfdata.p= rand()%265+10000;
    stsurfdata.u= rand()%101;
    stsurfdata.wd=rand()%361;
    stsurfdata.wf=rand()%151;
    stsurfdata.r= rand()%16;
    stsurfdata.vis=rand()%5001+100000;
    
    //把分钟观测数据的结构体放入vsurfdata容器
    vsurfdata.push_back(stsurfdata);
  }
}

//把容器vsurfdata中的全国气象站点分钟观测数据写入文件。
bool CrtSurfFile(const char* outpath, const char* datafmt)
{
  CFile File; 
  //拼接生成的数据文件名，例如：/tmp/surfdata/SURF_ZH_20220626152400_2200.csv
  char strFileName[301];
  sprintf(strFileName,"%s/SURF_ZH_%s_%d.%s",outpath,strddatetime,getpid(),datafmt);  
  
  //打开文件
  if(File.OpenForRename(strFileName,"w") == false)
  {
     logfile.Write("File.OpenForRename(%s) failed.\n",strFileName); return false;
  }
  //写入第一行标题
  if(strcmp(datafmt,"csv")==0) {File.Fprintf("站点代码，数据时间，气温，气压，相对湿度，风向，风速，降雨量，能见度。\n");} 
  //遍历vsurfdata容器并写入文件
  for(int ii=0 ; ii<vsurfdata.size(); ii++)
  {
     if(strcmp(datafmt,"csv")==0)
     {
       File.Fprintf("%s,%s,%.1f,%.1f,%d,%d,%.1f,%.1f,%.1f\n",vsurfdata[ii].obtid,vsurfdata[ii].ddatetime,vsurfdata[ii].t/10.0,vsurfdata[ii].p/10.0,\
       vsurfdata[ii].u, vsurfdata[ii].wd,vsurfdata[ii].wf/10.0,vsurfdata[ii].r/10.0,vsurfdata[ii].vis/10.0);
     }
  }  

  //关闭文件
  File.CloseAndRename();
  
  logfile.Write("生成数据文件%s成功 时间：%s 记录数：%d .\n",strFileName,strddatetime,vsurfdata.size());
  
  return true;
}






















