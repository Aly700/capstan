select json_build_object(
 'at',clock_timestamp(),
 'activity',(select coalesce(json_agg(a),'[]') from (select datname,state,wait_event_type,wait_event,count(*) as n from pg_stat_activity where backend_type='client backend' and pid<>pg_backend_pid() group by 1,2,3,4) a),
 'waiting_locks',(select count(*) from pg_locks l join pg_stat_activity a using(pid) where a.datname=current_database() and not l.granted),
 'lock_waits',(select coalesce(json_agg(a),'[]') from (select wait_event,pg_blocking_pids(pid) as blockers,left(query,120) as query from pg_stat_activity where datname=current_database() and wait_event_type='Lock') a),
 'tasks',(select json_build_object('total',count(*),'leased',count(*) filter(where leased_until is not null),'ready',count(*) filter(where leased_until is null)) from task),
 'database',(select row_to_json(d) from (select xact_commit,xact_rollback,blks_read,blks_hit,deadlocks from pg_stat_database where datname=current_database()) d)
);