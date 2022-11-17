delete from T_ZHOBTMIND  where ddatetime<timestampadd(minute,-30,now());
delete from T_ZHOBTMIND1 where ddatetime<timestampadd(minute,-30,now());
delete from T_ZHOBTMIND2 where ddatetime<timestampadd(minute,-30,now());
delete from T_ZHOBTMIND3 where ddatetime<timestampadd(minute,-30,now());


