--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.4
-- Dumped by pg_dump version 9.5.2

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: que; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA que;


ALTER SCHEMA que OWNER TO postgres;

SET search_path = que, pg_catalog;

--
-- Name: done(bigint, jsonb, bytea); Type: FUNCTION; Schema: que; Owner: postgres
--

CREATE FUNCTION done(id bigint, headers jsonb, body bytea) RETURNS void
    LANGUAGE plpgsql
    AS $$
/*
 Помечает сообщение как успешно обработанное
 Парметры - 
 id - ид сообщения
   Если такого сообщения нет, то будет выброщено исключение
   Если оно уже было обработано - выдастся предупреждение
 headers - http-заголовки запроса
 body - результат
 Ничего не возвращает
 -->>>Отправляем и проставляем, что все ок
 declare
  r que.que;
 begin
   delete from que.que;
   perform que.put('http://www.lala.com');
   select g.* into r from que.get() g;
   if not found then 
     raise exception 'Cannot get the message';
   end if;
   raise notice '%', r.id;
   perform que.done(r.id, '{"Content-type":"sql/postgres"}', 'Data goes here');
   if not exists(select * from que.que q 
                  where id=r.id 
                    and q.is_done 
                    and q.body='Data goes here' 
                    and q.headers='{"Content-type":"sql/postgres"}'
                 ) 
   then
     raise exception 'Cannot find expected done message';
   end if;
 end;
 --<<<
 
 -->>>!QU001!Non-existing message
 begin
  delete from que.que;
  perform que.done(-1, null, null);
 end;
 --<<<
*/
declare
 r que.que;
 begin
   select q.* into r from que.que q where q.id=done.id for update skip locked;
   if not found then
     raise sqlstate 'QU001' using message='Cannot find specified message. The message does not exists or not already locked by current backend';
   end if;
   if r.is_done then
     raise notice 'QU002' using message=format('Message (id=%s) already processed', id);
     return;
   end if;
   update que.que q set is_done=true, is_fail=false, tries_cnt=q.tries_cnt+1, headers=done.headers, body=done.body where q.id=done.id;
   if r.on_success is not null then
    begin    
     execute 'select ' || r.on_success using r.id;
    exception
     when others then
       update que.que q set errors = errors||sqlerrm where q.id=done.id;
    end; 
   end if;  
 end;
$$;


ALTER FUNCTION que.done(id bigint, headers jsonb, body bytea) OWNER TO postgres;

--
-- Name: fail(bigint, text, interval); Type: FUNCTION; Schema: que; Owner: postgres
--

CREATE FUNCTION fail(id bigint, error text DEFAULT NULL::text, delay interval DEFAULT make_interval(secs => (1)::double precision)) RETURNS void
    LANGUAGE plpgsql
    AS $$
/*
Отмечает неудачную попытку обработки сообщения
Параметры
 id - id сообщения
  Если такого сообшения нет, то будет выброшено исключение
 error - текст сообщения об ошибке
 delay - интервал, через который необходимо повторить попытку
-->>>Fail
declare
 r que.que;
begin
 delete from que.que;
 perform que.put('http://www.lala.com', max_tries_cnt=>2);
 select g.* into r from que.get() g;
 if not found then
   raise exception 'Cannod find new message';
 end if;
 perform que.fail(r.id, 'ERROR HERE', make_interval(secs:=0));
 if not exists(select * 
                 from que.que q 
                where q.id=r.id 
                  and q.errors=ARRAY['ERROR HERE'] 
                  and q.is_fail 
                  and not q.is_done 
                  and q.tries_cnt=1
               ) then
   raise exception 'Cannot find expected message';
 end if;
 perform que.fail(r.id, 'ERROR HERE2', make_interval(secs:=0));
 if not exists(select * from que.que q where q.id=r.id and q.errors=ARRAY['ERROR HERE', 'ERROR HERE2'] and q.is_fail and q.is_done and q.tries_cnt=2) then
   raise exception 'Cannot find expected message when done';
 end if;
end;
--<<<
*/
declare
 r que.que;
 begin
   select q.* into r from que.que q where q.id=fail.id for update skip locked;
   if not found then
     raise sqlstate 'QU001' using message='Cannot find specified message. The message does not exists or not already locked by current backend';
   end if;
   if r.is_done then
     raise notice '%', format('Message (id=%s) already processed', id);
     return;
   end if;
   update que.que q 
     set tries_cnt=q.tries_cnt+1, 
         is_fail=true, 
         deliver_after=case when delay is not null then now()+delay else deliver_after end,
         is_done=q.tries_cnt+1>=max_tries_cnt, 
         errors=case when error is null then errors else errors||error end 
   where q.id=fail.id;
   if r.on_fail is not null then
    begin    
     execute 'select ' || r.on_fail using r.id;
    exception
     when others then
       update que.que q set errors = errors||sqlerrm where q.id=fail.id;
    end; 
   end if;  
 end;
$$;


ALTER FUNCTION que.fail(id bigint, error text, delay interval) OWNER TO postgres;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: que; Type: TABLE; Schema: que; Owner: postgres
--

CREATE TABLE que (
    id bigint NOT NULL,
    is_done boolean DEFAULT false NOT NULL,
    is_fail boolean DEFAULT false NOT NULL,
    url text NOT NULL,
    tries_cnt integer DEFAULT 0 NOT NULL,
    max_tries_cnt integer DEFAULT 5 NOT NULL,
    deliver_after timestamp with time zone DEFAULT now() NOT NULL,
    errors text[],
    headers jsonb,
    body bytea,
    on_success text,
    on_fail text,
    param jsonb
);


ALTER TABLE que OWNER TO postgres;

--
-- Name: get(); Type: FUNCTION; Schema: que; Owner: postgres
--

CREATE FUNCTION get() RETURNS SETOF que
    LANGUAGE sql STABLE
    AS $$
/*
 Получает сообщения - то есть те строки, где не стоит флаг is_done и deliver_after<=now()
 Возвращает resultset в одну строку вида que.que, где строка - сообшение, готовое к получению
-->>>Reseive messages
declare
 r que.que;
 iid bigint;
begin
  perform que.put('http://www.facebook.com', max_tries_cnt=>512);
  iid:=lastval();
  select q.* into r from que.que q where q.id=iid;
  if r.url<>'http://www.facebook.com' or r.max_tries_cnt<>512 then
    raise exception 'Cannot get sended message';
  end if;
  perform from que.get();
  if not found then
    raise exception 'Cannot get any message';
  end if;
end;
--<<<
*/
 select q.* from que.que q where not q.is_done and q.deliver_after<=now() for update skip locked limit 1;
$$;


ALTER FUNCTION que.get() OWNER TO postgres;

--
-- Name: put(text, text, text, integer, timestamp with time zone, jsonb); Type: FUNCTION; Schema: que; Owner: postgres
--

CREATE FUNCTION put(url text, on_success text DEFAULT NULL::text, on_fail text DEFAULT NULL::text, max_tries_cnt integer DEFAULT 5, deliver_after timestamp with time zone DEFAULT now(), param jsonb DEFAULT NULL::jsonb) RETURNS void
    LANGUAGE sql
    AS $_$
/*
 Помещает запрос на http-запрос
 Параметры
 url - урл запроса 
 При удачном выполнении будет выполнена функция on_success
 При неудачном - on_fail
 И той и другой будет передан единственный аргумент - id вставляемой строки
 max_tries_cnt - число попыток
 deliver_after - сообщение будет получено ПОЗДНЕЕ указанного времени
   зачем это может потребоваться - если, например, сеть недоступна, то сообщение может быть выбрано указанное в
   max_tries_cnt число раз и помечено как неудачное, поэтому следующую попытку следует производить через какое-то время,
   и эта колонка обеспечивает соответствующее поведение
   param - дополнительные параметры для воркера
 Пример
-->>>Добавление сообщения
begin
  perform que.put('http://www.facebook.com', 
                    on_success=>'$1',
                    on_fail => '$1',
                    max_tries_cnt => 20,
                    deliver_after => now() + make_interval(mins=>5),
                    param => '{"method": "get"}'::jsonb
                );
end;
--<<<  

-->>>Добавление сообщения2
begin
  perform que.put('http://www.facebook.com');
end;
--<<<  

*/
 insert into que.que(url, max_tries_cnt, deliver_after, on_success, on_fail, param) values(put.url, put.max_tries_cnt, put.deliver_after, put.on_success, put.on_fail, put.param);
$_$;


ALTER FUNCTION que.put(url text, on_success text, on_fail text, max_tries_cnt integer, deliver_after timestamp with time zone, param jsonb) OWNER TO postgres;

--
-- Name: que_id_seq; Type: SEQUENCE; Schema: que; Owner: postgres
--

CREATE SEQUENCE que_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE que_id_seq OWNER TO postgres;

--
-- Name: que_id_seq; Type: SEQUENCE OWNED BY; Schema: que; Owner: postgres
--

ALTER SEQUENCE que_id_seq OWNED BY que.id;


--
-- Name: id; Type: DEFAULT; Schema: que; Owner: postgres
--

ALTER TABLE ONLY que ALTER COLUMN id SET DEFAULT nextval('que_id_seq'::regclass);


--
-- Name: que_pkey; Type: CONSTRAINT; Schema: que; Owner: postgres
--

ALTER TABLE ONLY que
    ADD CONSTRAINT que_pkey PRIMARY KEY (id);


--
-- Name: que_deliverafter_isdone; Type: INDEX; Schema: que; Owner: postgres
--

CREATE INDEX que_deliverafter_isdone ON que USING btree (deliver_after) WHERE (NOT is_done);


--
-- PostgreSQL database dump complete
--

