#include"_public.h"

void EXIT(int sig);

int main(int argc, char* argv[])
{
  if(argc != 4)
  {
     printf("\n");
     printf("Using:/project/tools1/bin/deletefiles pathname matchstr timeout\n\n");
     printf("Example:/project/tools1/bin/deletefiles /log/idc \"*.log.20*\" 0.02 \n");
     printf("        /project/tools1/bin/deletefiles /tmp/idc/surfdata \"*.xml,*.json\" 0.01 \n");
     printf("/project/tools/bin/procctl 300 /project/tools1/bin/deletefiles /log/idc \"*.log.20*\" 0.02 \n");
     printf("/project/tools/bin/procctl 300 /project/tools1/bin/deletefiles /tmp/idc/surfdata \"*.xml,*.json\" 0.01 \n\n");

     printf("这是一个工具程序，用于删除历史的数据文件或日志文件。\n");
     printf("本程序把pathname中timeout天之前的匹配matchstr的文件全部删除，timeout可以是小数.\n");
     printf("本程序不写日志，也不会在控制台输出任何信息.\n");
     return -1;
  }

  CloseIOAndSignal(true);
  signal(2,EXIT); signal(15,EXIT);

  //获取超时时间节点
  char strTimeOut[21];
  LocalTime(strTimeOut,"yyyy-mm-dd hh24:mi:ss",0-(int)(atof(argv[3])*60*60*24));

  //目录类
  CDir Dir;
  if(Dir.OpenDir(argv[1],argv[2],10000,true)==false)
  {
    printf("Dir.Open(%s) failed.\n",argv[1]); return -1; 
  }

  while(true)
  {
    if(Dir.ReadDir() == false) break;
   
    //如果比超时时间更早，且不是压缩文件
    if(strcmp(Dir.m_ModifyTime,strTimeOut) < 0)
    {
      if(REMOVE(Dir.m_FullFileName)==true)
       { printf("remove %s ok.\n",Dir.m_FullFileName);}
      else
       { printf("remove %s failed.\n",Dir.m_FullFileName);}        
    }
    
  }

  return 0;
}

void EXIT(int sig)
{
   printf("程序退出,sig=%d",sig);\
   exit(0);
}
