-- Tiempo promedio de respuesta de cada api

select b.api_name, round(b.t_por_api/count(a.api_name),2) as t_avg_resp from meli.process_timing a
inner join(
select sum(api_time) t_por_api , api_name from meli.process_timing
group by api_name
) b on b.api_name = a.api_name
group by a.api_name;

-- Tiempo de carga a la base 

select date_format(sys_dt,'%d%m%Y') AS load_dt, round(sum(db_time),2) as load_db_time from meli.process_timing
GROUP BY date_format(sys_dt,'%d%m%Y');

-- Tiempo total proceso

select  date_format(sys_dt,'%d%m%Y') AS load_process_dt, round(sum(complete_proc_time),2) as tot_proc_time from meli.process_timing
GROUP BY date_format(sys_dt,'%d%m%Y');

-- Cantidad de registros insertados diarios

select date_format(sys_dt,'%d%m%Y') AS FECHA,round(sum(PROCESS_ROWS)/3,0) as cant_rows_loaded 
from meli.process_timing
GROUP BY date_format(sys_dt,'%d%m%Y');