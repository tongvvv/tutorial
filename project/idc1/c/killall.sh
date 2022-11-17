killall -9 procctl
killall  gzipfiles crtsurfdata deletefiles ftpgetfiles ftpputfiles tcpputfiles tcpgetfiles fileserver
killall  obtcodetodb obtmindtodb execsql dminingmysql xmltodb syncupdate syncincrement 
killall  deletetable migratetable webserver


sleep 3

killall -9 gzipfiles crtsurfdata deletefiles ftpgetfiles ftpputfiles tcpputfiles tcpgetfiles fileserver
killall -9 obtcodetodb obtmindtodb execsql dminingmysql xmltodb syncupdate syncincrement
killall -9 deletetable migratetable webserver


