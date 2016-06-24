set statement_timeout = 0;
set lock_timeout = 0;
set client_encoding = 'UTF8';
set standard_conforming_strings = on;
set check_function_bodies = false;
set client_min_messages = warning;
set row_security = off;

--
-- Name: que; Type: SCHEMA; Schema: -; Owner: postgres
--

create schema que;

alter schema que
owner to postgres;

set search_path = que, pg_catalog;

--
-- Name: done(bigint, jsonb, bytea); Type: FUNCTION; Schema: que; Owner: postgres
--

create function done(id bigint, headers jsonb, body bytea)
    returns void
language plpgsql
as $$
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
    select q.*
    into r
    from que.que q
    where q.id = done.id
    for update skip locked;

    if not found
    then
        raise sqlstate 'QU001'
        using message ='Cannot find specified message. The message does not exists or not already locked by current backend';
    end if;

    if r.is_done
    then
        raise notice 'QU002'
        using message =format('Message (id=%s) already processed', id);
        return;
    end if;

    update que.que q
    set
        is_done   = true,
        is_fail   = false,
        tries_cnt = q.tries_cnt + 1,
        headers   = done.headers,
        body      = done.body
    where q.id = done.id;

    if r.on_fail is not null
    then
        begin
            execute 'select ' || r.on_success
            using r.id;

            exception
            when others
                then
                    update que.que q
                    set
                        errors = errors || sqlerrm
                    where q.id = done.id;
        end;
    end if;
end;
$$;


alter function que.done(id bigint, headers jsonb, body bytea )
owner to postgres;

--
-- Name: fail(bigint, text, interval); Type: FUNCTION; Schema: que; Owner: postgres
--

create function fail(
    id    bigint,
    error text default null :: text,
    delay interval default make_interval(secs => (1) :: double precision)
)
    returns void
language plpgsql
as $$
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
    select q.*
    into r
    from que.que q
    where q.id = fail.id
    for update skip locked;

    if not found
    then
        raise sqlstate 'QU001'
        using message ='Cannot find specified message. The message does not exists or not already locked by current backend';
    end if;

    if r.is_done
    then
        raise notice '%', format('Message (id=%s) already processed', id);
        return;
    end if;


    update que.que q
    set
        tries_cnt     = q.tries_cnt + 1,
        is_fail       = true,
        deliver_after = case
                        when delay is not null
                            then now() + delay
                        else deliver_after
                        end,
        is_done       = q.tries_cnt + 1 >= max_tries_cnt,
        errors        = case
                        when error is null
                            then errors
                        else errors || error
                        end
    where q.id = fail.id;

    if r.on_fail is not null
    then
        begin
            execute 'select ' || r.on_fail
            using r.id;
            exception
            when others
                then
                    update que.que q
                    set
                        errors = errors || sqlerrm
                    where q.id = fail.id;
        end;
    end if;
end;
$$;


alter function que.fail(id bigint, error text, delay interval )
owner to postgres;

set default_tablespace = '';

set default_with_oids = false;

--
-- Name: que; Type: TABLE; Schema: que; Owner: postgres
--

create table que (
    id            bigint                                 not null,
    is_done       boolean default false                  not null,
    is_fail       boolean default false                  not null,
    url           text                                   not null,
    tries_cnt     integer default 0                      not null,
    max_tries_cnt integer default 5                      not null,
    deliver_after timestamp with time zone default now() not null,
    errors        text [],
    headers       jsonb,
    body          bytea,
    on_success    text,
    on_fail       text,
    param         jsonb
);


alter table que
    owner to postgres;

--
-- Name: get(); Type: FUNCTION; Schema: que; Owner: postgres
--

create function get()
    returns setof que
language sql stable
as $$
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
select q.*
from que.que q
where not q.is_done and q.deliver_after <= now()
for update skip locked limit 1;
$$;


alter function que.get()
owner to postgres;

--
-- Name: put(text, text, text, integer, timestamp with time zone, jsonb); Type: FUNCTION; Schema: que; Owner: postgres
--

create function put(url           text,
                    on_success    text default null :: text,
                    on_fail       text default null :: text,
                    max_tries_cnt integer default 5,
                    deliver_after timestamp with time zone default now(),
                    param         jsonb default null :: jsonb)
    returns void
language sql
as $_$
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
insert into que.que (url, max_tries_cnt, deliver_after, on_success, on_fail, param)
values (put.url, put.max_tries_cnt, put.deliver_after, put.on_success, put.on_fail, put.param);
$_$;


alter function que.put(url text, on_success text, on_fail text, max_tries_cnt integer, deliver_after timestamp with time zone, param jsonb )
owner to postgres;

--
-- Name: que_id_seq; Type: SEQUENCE; Schema: que; Owner: postgres
--

create sequence que_id_seq
start with 1
increment by 1
no minvalue
no maxvalue
cache 1;


alter table que_id_seq
    owner to postgres;

--
-- Name: que_id_seq; Type: SEQUENCE OWNED BY; Schema: que; Owner: postgres
--

alter sequence que_id_seq owned by que.id;

--
-- Name: id; Type: DEFAULT; Schema: que; Owner: postgres
--

alter table only que
    alter column id set default nextval('que_id_seq' :: regclass);

--
-- Name: que_pkey; Type: CONSTRAINT; Schema: que; Owner: postgres
--

alter table only que
    add constraint que_pkey primary key (id);

--
-- Name: que_deliverafter_isdone; Type: INDEX; Schema: que; Owner: postgres
--

create index que_deliverafter_isdone on que using btree (deliver_after)
    where (not is_done);
