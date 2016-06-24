-- Function: public.test_a_routine(text)

-- DROP FUNCTION public.test_a_routine(text);

create or replace function public.test_a_routine(in fname text)
    returns table(routine text, testname text, result text, passed boolean, failed boolean, error_message text) as
$BODY$
/*
 Тестируем функции.
 Параметр: название функции в виде "название" или "схема.название". "Название" может быть шаблоном для like.
 Тесты берутся из комменариев в тексте функций. Тест начинается со строки
 -->>>Название теста 
 --   или    
 -->>>!маска для like ожидаемого исключения!Название теста 
 -- Тут собственно тест
 -- тест всего лишь не должен выбросить исключений
 -- если не выбросил - то все хорошо, если выбросил - тест не пройден
 begin
   perform 1;
 end;
 --<<<

 Тестов можен быть несколько
 -->>>Проверяем присванивание
  declare
   v text;
   begin
    v=100;
    if v::integer<>100 then
      raise exception 'Strange behavior of assignment';
    end if;
  end;
  --<<<

  -->>>!ER999!Тут мы ожидаем получить исключение
  declare
   i int;
  begin
    i=100;
    if i>1 then
      raise sqlstate 'ER999' using message='Exception';
    end if;
  end;
 --<<<

 -->>>!ER999!Тут мы ожидаем получить исключение, но у нас другое
  declare
   i int;
  begin
    i=100;
    if i>1 then
      raise sqlstate 'ER998' using message='Exception';
    end if;
  end;
 --<<<

 -->>>!%!Тут мы ожидаем получить любое исключение
  declare
   i int;
  begin
    i=100;
    if i>1 then
      raise sqlstate 'ER998' using message='Exception';
    end if;
  end;
 --<<<

 -->>>!%!Тут мы ожидаем получить любое исключение, но не получаем никакого
  declare
   i int;
  begin
    i=100;
  end;
 --<<<

  Кроме того, текст процедуры исследуется на предмет наличия контрольных точек вида / *#имя-точки * / - см. пример ниже в коде -
  и функция следит, чтобы управление прошло через каждую как минимум один раз.
  Учет прохождения осуществляется через advisory locks, если что.

  Все тесты для одной функции выполняются в одной транзакции; после выполнения тестов для этой функции транзакция для
  нее откатывается; таким образом, по завершению всех тестов откатываются все транзакции.
*/
declare
    src           text;
    matches       text [];
    rs            record;
    rname         text = coalesce(substring(fname from $RE$\.(.+)$RE$), fname);
    sname         text = coalesce(substring(fname from $RE$^([^.]*)\.$RE$), 'public');
    r             record;
    result        text;
    controlpoints jsonb;
begin
/*#start */
    if rname is null
    then
/*#wrong-name */
        raise exception 'Wrong function name:%', fname;
    end if;


    for rs in (
        select
            routine_definition as def,
            ro.specific_schema,
            ro.routine_name
        from information_schema.routines ro
        where ro.specific_schema = sname and routine_name like rname
    ) loop
        begin
            passed = null;
            failed = null;
            error_message = 'No tests available';
            routine = rs.specific_schema || '.' || rs.routine_name;
            testname = null;

            select jsonb_object_agg(v, cnt)
            into controlpoints
            from (
                     select
                         v [1]    as v,
                         count(*) as cnt
                     from regexp_matches(rs.def, $R$/\*#([-_0-9a-zA-Z]+)?(?:[^*]|\*[^/])*\*/$R$, 'g') as rm(v)
                     group by 1
                 ) as t;

            src = pg_get_functiondef((rs.specific_schema || '.' || rs.routine_name) :: regproc :: oid);
            src = regexp_replace(
                src,
                $R$/\*#([-_0-9a-zA-Z]+)?(?:[^*]|\*[^/])*\*/$R$,
                format(
                    $Q$perform pg_advisory_lock(hashtext('testing.result.%s.\1')::bigint);$Q$,
                    rs.specific_schema || '.' || rs.routine_name
                ),
                'g'
            );
            execute src;

            for r in (
                select *
                from regexp_matches(
                         rs.def,
                         '[^\n]*\n?\s*-->>>(?:!(\S+)!)?([^\n]*)\n((.(?!--<<<))+)\s*--<<<[^\n]*\n',
                         'g'
                     ) as ms(s)
            ) loop
                passed = true;
                failed = false;
                error_message = null;
                testname = r.s [2];
                begin
                    execute 'do $theroutenecodegoeshere$ ' || r.s [3] || ' $theroutenecodegoeshere$;';
                    if nullif(r.s [1], '') is not null
                    then
                        passed = false;
                        failed = true;
                        error_message = 'Expected exception ' || r.s [1] || ' has not been got';
                    end if;
                    exception
                    when others
                        then
                            if nullif(r.s [1], '') is null
                            then
                                passed = false;
                                failed = true;
                                error_message = sqlerrm || ' sqlstate:' || sqlstate;
                            else
                                if sqlstate not like r.s [1]
                                then
                                    passed = false;
                                    failed = true;
                                    error_message = sqlerrm || ' sqlstate:' || sqlstate;
                                end if;
                            end if;
                end;
                return next;
            end loop;
            return query
            select
                rs.specific_schema || '.' || rs.routine_name,
                '*** Control point:' :: text || k,
                case when
                    exists(
                        select *
                        from pg_locks l
                        where
                            l.objid = hashtext(
                                'testing.result.' || rs.specific_schema || '.' || rs.routine_name || '.' || k
                            ) :: oid
                    )
                    then 'OK'
                else 'ERROR'
                end,
                case when
                    exists(
                        select *
                        from pg_locks l
                        where
                            l.objid = hashtext(
                                'testing.result.' || rs.specific_schema || '.' || rs.routine_name || '.' || k
                            ) :: oid
                    )
                    then true
                else false
                end,
                case when
                    exists(
                        select *
                        from pg_locks l
                        where
                            l.objid = hashtext(
                                'testing.result.' || rs.specific_schema || '.' || rs.routine_name || '.' || k
                            ) :: oid
                    )
                    then
                        false
                else true end,
                case when
                    exists(
                        select *
                        from pg_locks l
                        where
                            l.objid = hashtext(
                                'testing.result.' || rs.specific_schema || '.' || rs.routine_name || '.' || k
                            ) :: oid
                    )
                    then ''
                else format('Control point %s has not been reached', k)
                end
            from jsonb_each(controlpoints) as je(k, v);
            raise exception sqlstate 'RB999';
            exception
            when sqlstate 'RB999'
                then null;
        end;
    end loop;
    perform pg_advisory_unlock_all();
    exception
    when others
        then perform pg_advisory_unlock_all();
            raise;
end;
$BODY$
language plpgsql volatile
cost 100
rows 1000;
alter function public.test_a_routine( text )
owner to postgres;